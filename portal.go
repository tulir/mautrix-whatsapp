// mautrix-whatsapp - A Matrix-WhatsApp puppeting bridge.
// Copyright (C) 2020 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io/ioutil"
	"math"
	"math/rand"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	log "maunium.net/go/maulogger/v2"

	"maunium.net/go/mautrix/crypto/attachment"

	"github.com/Rhymen/go-whatsapp"
	waProto "github.com/Rhymen/go-whatsapp/binary/proto"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"
	"maunium.net/go/mautrix/pushrules"

	"maunium.net/go/mautrix-whatsapp/database"
	"maunium.net/go/mautrix-whatsapp/types"
	"maunium.net/go/mautrix-whatsapp/whatsapp-ext"
)

func (bridge *Bridge) GetPortalByMXID(mxid id.RoomID) *Portal {
	bridge.portalsLock.Lock()
	defer bridge.portalsLock.Unlock()
	portal, ok := bridge.portalsByMXID[mxid]
	if !ok {
		return bridge.loadDBPortal(bridge.DB.Portal.GetByMXID(mxid), nil)
	}
	return portal
}

func (bridge *Bridge) GetPortalByJID(key database.PortalKey) *Portal {
	bridge.portalsLock.Lock()
	defer bridge.portalsLock.Unlock()
	portal, ok := bridge.portalsByJID[key]
	if !ok {
		return bridge.loadDBPortal(bridge.DB.Portal.GetByJID(key), &key)
	}
	return portal
}

func (bridge *Bridge) GetAllPortals() []*Portal {
	return bridge.dbPortalsToPortals(bridge.DB.Portal.GetAll())
}

func (bridge *Bridge) GetAllPortalsByJID(jid types.WhatsAppID) []*Portal {
	return bridge.dbPortalsToPortals(bridge.DB.Portal.GetAllByJID(jid))
}

func (bridge *Bridge) dbPortalsToPortals(dbPortals []*database.Portal) []*Portal {
	bridge.portalsLock.Lock()
	defer bridge.portalsLock.Unlock()
	output := make([]*Portal, len(dbPortals))
	for index, dbPortal := range dbPortals {
		if dbPortal == nil {
			continue
		}
		portal, ok := bridge.portalsByJID[dbPortal.Key]
		if !ok {
			portal = bridge.loadDBPortal(dbPortal, nil)
		}
		output[index] = portal
	}
	return output
}

func (bridge *Bridge) loadDBPortal(dbPortal *database.Portal, key *database.PortalKey) *Portal {
	if dbPortal == nil {
		if key == nil {
			return nil
		}
		dbPortal = bridge.DB.Portal.New()
		dbPortal.Key = *key
		dbPortal.Insert()
	}
	portal := bridge.NewPortal(dbPortal)
	bridge.portalsByJID[portal.Key] = portal
	if len(portal.MXID) > 0 {
		bridge.portalsByMXID[portal.MXID] = portal
	}
	return portal
}

func (portal *Portal) GetUsers() []*User {
	return nil
}

func (bridge *Bridge) NewManualPortal(key database.PortalKey) *Portal {
	portal := &Portal{
		Portal: bridge.DB.Portal.New(),
		bridge: bridge,
		log:    bridge.Log.Sub(fmt.Sprintf("Portal/%s", key)),

		recentlyHandled: [recentlyHandledLength]types.WhatsAppMessageID{},

		messages: make(chan PortalMessage, bridge.Config.Bridge.PortalMessageBuffer),
	}
	portal.Key = key
	go portal.handleMessageLoop()
	return portal
}

func (bridge *Bridge) NewPortal(dbPortal *database.Portal) *Portal {
	portal := &Portal{
		Portal: dbPortal,
		bridge: bridge,
		log:    bridge.Log.Sub(fmt.Sprintf("Portal/%s", dbPortal.Key)),

		recentlyHandled: [recentlyHandledLength]types.WhatsAppMessageID{},

		messages: make(chan PortalMessage, bridge.Config.Bridge.PortalMessageBuffer),
	}
	go portal.handleMessageLoop()
	return portal
}

const recentlyHandledLength = 100

type PortalMessage struct {
	chat      string
	source    *User
	data      interface{}
	timestamp uint64
}

type Portal struct {
	*database.Portal

	bridge *Bridge
	log    log.Logger

	roomCreateLock sync.Mutex

	recentlyHandled      [recentlyHandledLength]types.WhatsAppMessageID
	recentlyHandledLock  sync.Mutex
	recentlyHandledIndex uint8

	backfillLock  sync.Mutex
	backfilling   bool
	lastMessageTs uint64

	privateChatBackfillInvitePuppet func()

	messages chan PortalMessage

	isPrivate   *bool
	hasRelaybot *bool
}

const MaxMessageAgeToCreatePortal = 5 * 60 // 5 minutes

func (portal *Portal) handleMessageLoop() {
	for msg := range portal.messages {
		if len(portal.MXID) == 0 {
			if msg.timestamp+MaxMessageAgeToCreatePortal < uint64(time.Now().Unix()) {
				portal.log.Debugln("Not creating portal room for incoming message as the message is too old.")
				continue
			}
			portal.log.Debugln("Creating Matrix room from incoming message")
			err := portal.CreateMatrixRoom(msg.source)
			if err != nil {
				portal.log.Errorln("Failed to create portal room:", err)
				return
			}
		}
		portal.backfillLock.Lock()
		portal.handleMessage(msg)
		portal.backfillLock.Unlock()
	}
}

func (portal *Portal) handleMessage(msg PortalMessage) {
	if len(portal.MXID) == 0 {
		portal.log.Warnln("handleMessage called even though portal.MXID is empty")
		return
	}
	switch data := msg.data.(type) {
	case whatsapp.TextMessage:
		portal.HandleTextMessage(msg.source, data)
	case whatsapp.ImageMessage:
		portal.HandleMediaMessage(msg.source, mediaMessage{
			base:      base{data.Download, data.Info, data.ContextInfo, data.Type},
			thumbnail: data.Thumbnail,
			caption:   data.Caption,
		})
	case whatsapp.StickerMessage:
		portal.HandleMediaMessage(msg.source, mediaMessage{
			base:          base{data.Download, data.Info, data.ContextInfo, data.Type},
			sendAsSticker: true,
		})
	case whatsapp.VideoMessage:
		portal.HandleMediaMessage(msg.source, mediaMessage{
			base:      base{data.Download, data.Info, data.ContextInfo, data.Type},
			thumbnail: data.Thumbnail,
			caption:   data.Caption,
			length:    data.Length,
		})
	case whatsapp.AudioMessage:
		portal.HandleMediaMessage(msg.source, mediaMessage{
			base:   base{data.Download, data.Info, data.ContextInfo, data.Type},
			length: data.Length,
		})
	case whatsapp.DocumentMessage:
		fileName := data.FileName
		if len(fileName) == 0 {
			fileName = data.Title
		}
		portal.HandleMediaMessage(msg.source, mediaMessage{
			base:      base{data.Download, data.Info, data.ContextInfo, data.Type},
			thumbnail: data.Thumbnail,
			fileName:  fileName,
		})
	case whatsapp.ContactMessage:
		portal.HandleContactMessage(msg.source, data)
	case whatsapp.LocationMessage:
		portal.HandleLocationMessage(msg.source, data)
	case whatsappExt.MessageRevocation:
		portal.HandleMessageRevoke(msg.source, data)
	case FakeMessage:
		portal.HandleFakeMessage(msg.source, data)
	default:
		portal.log.Warnln("Unknown message type:", reflect.TypeOf(msg.data))
	}
}

func (portal *Portal) isRecentlyHandled(id types.WhatsAppMessageID) bool {
	start := portal.recentlyHandledIndex
	for i := start; i != start; i = (i - 1) % recentlyHandledLength {
		if portal.recentlyHandled[i] == id {
			return true
		}
	}
	return false
}

func (portal *Portal) isDuplicate(id types.WhatsAppMessageID) bool {
	msg := portal.bridge.DB.Message.GetByJID(portal.Key, id)
	if msg != nil {
		return true
	}
	return false
}

func init() {
	gob.Register(&waProto.Message{})
}

func (portal *Portal) markHandled(source *User, message *waProto.WebMessageInfo, mxid id.EventID) {
	msg := portal.bridge.DB.Message.New()
	msg.Chat = portal.Key
	msg.JID = message.GetKey().GetId()
	msg.MXID = mxid
	msg.Timestamp = message.GetMessageTimestamp()
	if message.GetKey().GetFromMe() {
		msg.Sender = source.JID
	} else if portal.IsPrivateChat() {
		msg.Sender = portal.Key.JID
	} else {
		msg.Sender = message.GetKey().GetParticipant()
		if len(msg.Sender) == 0 {
			msg.Sender = message.GetParticipant()
		}
	}
	msg.Content = message.Message
	msg.Insert()

	portal.recentlyHandledLock.Lock()
	index := portal.recentlyHandledIndex
	portal.recentlyHandledIndex = (portal.recentlyHandledIndex + 1) % recentlyHandledLength
	portal.recentlyHandledLock.Unlock()
	portal.recentlyHandled[index] = msg.JID
}

func (portal *Portal) getMessageIntent(user *User, info whatsapp.MessageInfo) *appservice.IntentAPI {
	if info.FromMe {
		return portal.bridge.GetPuppetByJID(user.JID).IntentFor(portal)
	} else if portal.IsPrivateChat() {
		return portal.MainIntent()
	} else if len(info.SenderJid) == 0 {
		if len(info.Source.GetParticipant()) != 0 {
			info.SenderJid = info.Source.GetParticipant()
		} else {
			return nil
		}
	}
	return portal.bridge.GetPuppetByJID(info.SenderJid).IntentFor(portal)
}

func (portal *Portal) startHandling(source *User, info whatsapp.MessageInfo) *appservice.IntentAPI {
	// TODO these should all be trace logs
	if portal.lastMessageTs > info.Timestamp+1 {
		portal.log.Debugfln("Not handling %s: message is older (%d) than last bridge message (%d)", info.Id, info.Timestamp, portal.lastMessageTs)
	} else if portal.isRecentlyHandled(info.Id) {
		portal.log.Debugfln("Not handling %s: message was recently handled", info.Id)
	} else if portal.isDuplicate(info.Id) {
		portal.log.Debugfln("Not handling %s: message is duplicate", info.Id)
	} else {
		portal.lastMessageTs = info.Timestamp
		intent := portal.getMessageIntent(source, info)
		if intent != nil {
			portal.log.Debugfln("Starting handling of %s (ts: %d)", info.Id, info.Timestamp)
		} else {
			portal.log.Debugfln("Not handling %s: sender is not known")
		}
		return intent
	}
	return nil
}

func (portal *Portal) finishHandling(source *User, message *waProto.WebMessageInfo, mxid id.EventID) {
	portal.markHandled(source, message, mxid)
	portal.sendDeliveryReceipt(mxid)
	portal.log.Debugln("Handled message", message.GetKey().GetId(), "->", mxid)
}

func (portal *Portal) SyncParticipants(metadata *whatsappExt.GroupInfo) {
	changed := false
	levels, err := portal.MainIntent().PowerLevels(portal.MXID)
	if err != nil {
		levels = portal.GetBasePowerLevels()
		changed = true
	}
	participantMap := make(map[string]bool)
	for _, participant := range metadata.Participants {
		participantMap[participant.JID] = true
		user := portal.bridge.GetUserByJID(participant.JID)
		portal.userMXIDAction(user, portal.ensureMXIDInvited)

		puppet := portal.bridge.GetPuppetByJID(participant.JID)
		err := puppet.IntentFor(portal).EnsureJoined(portal.MXID)
		if err != nil {
			portal.log.Warnfln("Failed to make puppet of %s join %s: %v", participant.JID, portal.MXID, err)
		}

		expectedLevel := 0
		if participant.IsSuperAdmin {
			expectedLevel = 95
		} else if participant.IsAdmin {
			expectedLevel = 50
		}
		changed = levels.EnsureUserLevel(puppet.MXID, expectedLevel) || changed
		if user != nil {
			changed = levels.EnsureUserLevel(user.MXID, expectedLevel) || changed
		}
	}
	if changed {
		_, err = portal.MainIntent().SetPowerLevels(portal.MXID, levels)
		if err != nil {
			portal.log.Errorln("Failed to change power levels:", err)
		}
	}
	members, err := portal.MainIntent().JoinedMembers(portal.MXID)
	if err != nil {
		portal.log.Warnln("Failed to get member list:", err)
	} else {
		for member := range members.Joined {
			jid, ok := portal.bridge.ParsePuppetMXID(member)
			if ok {
				_, shouldBePresent := participantMap[jid]
				if !shouldBePresent {
					_, err := portal.MainIntent().KickUser(portal.MXID, &mautrix.ReqKickUser{
						UserID: member,
						Reason: "User had left this WhatsApp chat",
					})
					if err != nil {
						portal.log.Warnfln("Failed to kick user %s who had left: %v", member, err)
					}
				}
			}
		}
	}
}

func (portal *Portal) UpdateAvatar(user *User, avatar *whatsappExt.ProfilePicInfo, updateInfo bool) bool {
	if avatar == nil {
		var err error
		avatar, err = user.Conn.GetProfilePicThumb(portal.Key.JID)
		if err != nil {
			portal.log.Errorln(err)
			return false
		}
	}

	if avatar.Status != 0 {
		return false
	}

	if portal.Avatar == avatar.Tag {
		return false
	}

	data, err := avatar.DownloadBytes()
	if err != nil {
		portal.log.Warnln("Failed to download avatar:", err)
		return false
	}

	mimeType := http.DetectContentType(data)
	resp, err := portal.MainIntent().UploadBytes(data, mimeType)
	if err != nil {
		portal.log.Warnln("Failed to upload avatar:", err)
		return false
	}

	portal.AvatarURL = resp.ContentURI
	if len(portal.MXID) > 0 {
		_, err = portal.MainIntent().SetRoomAvatar(portal.MXID, resp.ContentURI)
		if err != nil {
			portal.log.Warnln("Failed to set room topic:", err)
			return false
		}
	}
	portal.Avatar = avatar.Tag
	if updateInfo {
		portal.UpdateBridgeInfo()
	}
	return true
}

func (portal *Portal) UpdateName(name string, setBy types.WhatsAppID, updateInfo bool) bool {
	if portal.Name != name {
		intent := portal.MainIntent()
		if len(setBy) > 0 {
			intent = portal.bridge.GetPuppetByJID(setBy).IntentFor(portal)
		}
		_, err := intent.SetRoomName(portal.MXID, name)
		if err == nil {
			portal.Name = name
			if updateInfo {
				portal.UpdateBridgeInfo()
			}
			return true
		}
		portal.log.Warnln("Failed to set room name:", err)
	}
	return false
}

func (portal *Portal) UpdateTopic(topic string, setBy types.WhatsAppID, updateInfo bool) bool {
	if portal.Topic != topic {
		intent := portal.MainIntent()
		if len(setBy) > 0 {
			intent = portal.bridge.GetPuppetByJID(setBy).IntentFor(portal)
		}
		_, err := intent.SetRoomTopic(portal.MXID, topic)
		if err == nil {
			portal.Topic = topic
			if updateInfo {
				portal.UpdateBridgeInfo()
			}
			return true
		}
		portal.log.Warnln("Failed to set room topic:", err)
	}
	return false
}

func (portal *Portal) UpdateMetadata(user *User) bool {
	if portal.IsPrivateChat() {
		return false
	} else if portal.IsStatusBroadcastRoom() {
		update := false
		update = portal.UpdateName("WhatsApp Status Broadcast", "", false) || update
		update = portal.UpdateTopic("WhatsApp status updates from your contacts", "", false) || update
		return update
	}
	metadata, err := user.Conn.GetGroupMetaData(portal.Key.JID)
	if err != nil {
		portal.log.Errorln(err)
		return false
	}
	if metadata.Status != 0 {
		// 401: access denied
		// 404: group does (no longer) exist
		// 500: ??? happens with status@broadcast

		// TODO: update the room, e.g. change priority level
		//   to send messages to moderator
		return false
	}

	portal.SyncParticipants(metadata)
	update := false
	update = portal.UpdateName(metadata.Name, metadata.NameSetBy, false) || update
	update = portal.UpdateTopic(metadata.Topic, metadata.TopicSetBy, false) || update

	portal.RestrictMessageSending(metadata.Announce)

	return update
}

func (portal *Portal) userMXIDAction(user *User, fn func(mxid id.UserID)) {
	if user == nil {
		return
	}

	if user == portal.bridge.Relaybot {
		for _, mxid := range portal.bridge.Config.Bridge.Relaybot.InviteUsers {
			fn(mxid)
		}
	} else {
		fn(user.MXID)
	}
}

func (portal *Portal) ensureMXIDInvited(mxid id.UserID) {
	err := portal.MainIntent().EnsureInvited(portal.MXID, mxid)
	if err != nil {
		portal.log.Warnfln("Failed to ensure %s is invited to %s: %v", mxid, portal.MXID, err)
	}
}

func (portal *Portal) ensureUserInvited(user *User) {
	portal.userMXIDAction(user, portal.ensureMXIDInvited)

	customPuppet := portal.bridge.GetPuppetByCustomMXID(user.MXID)
	if customPuppet != nil && customPuppet.CustomIntent() != nil {
		_ = customPuppet.CustomIntent().EnsureJoined(portal.MXID)
	}
}

func (portal *Portal) Sync(user *User, contact whatsapp.Contact) {
	portal.log.Infoln("Syncing portal for", user.MXID)

	if user.IsRelaybot {
		yes := true
		portal.hasRelaybot = &yes
	}

	if len(portal.MXID) == 0 {
		if !portal.IsPrivateChat() {
			portal.Name = contact.Name
		}
		err := portal.CreateMatrixRoom(user)
		if err != nil {
			portal.log.Errorln("Failed to create portal room:", err)
			return
		}
	} else {
		portal.ensureUserInvited(user)
	}

	if portal.IsPrivateChat() {
		return
	}

	update := false
	update = portal.UpdateMetadata(user) || update
	if !portal.IsStatusBroadcastRoom() {
		update = portal.UpdateAvatar(user, nil, false) || update
	}
	if update {
		portal.Update()
		portal.UpdateBridgeInfo()
	}
}

func (portal *Portal) GetBasePowerLevels() *event.PowerLevelsEventContent {
	anyone := 0
	nope := 99
	invite := 50
	if portal.bridge.Config.Bridge.AllowUserInvite {
		invite = 0
	}
	return &event.PowerLevelsEventContent{
		UsersDefault:    anyone,
		EventsDefault:   anyone,
		RedactPtr:       &anyone,
		StateDefaultPtr: &nope,
		BanPtr:          &nope,
		InvitePtr:       &invite,
		Users: map[id.UserID]int{
			portal.MainIntent().UserID: 100,
		},
		Events: map[string]int{
			event.StateRoomName.Type:   anyone,
			event.StateRoomAvatar.Type: anyone,
			event.StateTopic.Type:      anyone,
		},
	}
}

func (portal *Portal) ChangeAdminStatus(jids []string, setAdmin bool) {
	levels, err := portal.MainIntent().PowerLevels(portal.MXID)
	if err != nil {
		levels = portal.GetBasePowerLevels()
	}
	newLevel := 0
	if setAdmin {
		newLevel = 50
	}
	changed := false
	for _, jid := range jids {
		puppet := portal.bridge.GetPuppetByJID(jid)
		changed = levels.EnsureUserLevel(puppet.MXID, newLevel) || changed

		user := portal.bridge.GetUserByJID(jid)
		if user != nil {
			changed = levels.EnsureUserLevel(user.MXID, newLevel) || changed
		}
	}
	if changed {
		_, err = portal.MainIntent().SetPowerLevels(portal.MXID, levels)
		if err != nil {
			portal.log.Errorln("Failed to change power levels:", err)
		}
	}
}

func (portal *Portal) RestrictMessageSending(restrict bool) {
	levels, err := portal.MainIntent().PowerLevels(portal.MXID)
	if err != nil {
		levels = portal.GetBasePowerLevels()
	}

	newLevel := 0
	if restrict {
		newLevel = 50
	}

	if levels.EventsDefault == newLevel {
		return
	}

	levels.EventsDefault = newLevel
	_, err = portal.MainIntent().SetPowerLevels(portal.MXID, levels)
	if err != nil {
		portal.log.Errorln("Failed to change power levels:", err)
	}
}

func (portal *Portal) RestrictMetadataChanges(restrict bool) {
	levels, err := portal.MainIntent().PowerLevels(portal.MXID)
	if err != nil {
		levels = portal.GetBasePowerLevels()
	}
	newLevel := 0
	if restrict {
		newLevel = 50
	}
	changed := false
	changed = levels.EnsureEventLevel(event.StateRoomName, newLevel) || changed
	changed = levels.EnsureEventLevel(event.StateRoomAvatar, newLevel) || changed
	changed = levels.EnsureEventLevel(event.StateTopic, newLevel) || changed
	if changed {
		_, err = portal.MainIntent().SetPowerLevels(portal.MXID, levels)
		if err != nil {
			portal.log.Errorln("Failed to change power levels:", err)
		}
	}
}

func (portal *Portal) BackfillHistory(user *User, lastMessageTime uint64) error {
	if !portal.bridge.Config.Bridge.RecoverHistory {
		return nil
	}

	endBackfill := portal.beginBackfill()
	defer endBackfill()

	lastMessage := portal.bridge.DB.Message.GetLastInChat(portal.Key)
	if lastMessage == nil {
		return nil
	}
	if lastMessage.Timestamp >= lastMessageTime {
		portal.log.Debugln("Not backfilling: no new messages")
		return nil
	}

	lastMessageID := lastMessage.JID
	lastMessageFromMe := lastMessage.Sender == user.JID
	portal.log.Infoln("Backfilling history since", lastMessageID, "for", user.MXID)
	for len(lastMessageID) > 0 {
		portal.log.Debugln("Fetching 50 messages of history after", lastMessageID)
		resp, err := user.Conn.LoadMessagesAfter(portal.Key.JID, lastMessageID, lastMessageFromMe, 50)
		if err != nil {
			return err
		}
		messages, ok := resp.Content.([]interface{})
		if !ok || len(messages) == 0 {
			portal.log.Debugfln("Didn't get more messages to backfill (resp.Content is %T)", resp.Content)
			break
		}

		portal.handleHistory(user, messages)

		lastMessageProto, ok := messages[len(messages)-1].(*waProto.WebMessageInfo)
		if ok {
			lastMessageID = lastMessageProto.GetKey().GetId()
			lastMessageFromMe = lastMessageProto.GetKey().GetFromMe()
		}
	}
	portal.log.Infoln("Backfilling finished")
	return nil
}

func (portal *Portal) beginBackfill() func() {
	portal.backfillLock.Lock()
	portal.backfilling = true
	var privateChatPuppetInvited bool
	var privateChatPuppet *Puppet
	if portal.IsPrivateChat() && portal.bridge.Config.Bridge.InviteOwnPuppetForBackfilling && portal.Key.JID != portal.Key.Receiver {
		privateChatPuppet = portal.bridge.GetPuppetByJID(portal.Key.Receiver)
		portal.privateChatBackfillInvitePuppet = func() {
			if privateChatPuppetInvited {
				return
			}
			privateChatPuppetInvited = true
			_, _ = portal.MainIntent().InviteUser(portal.MXID, &mautrix.ReqInviteUser{UserID: privateChatPuppet.MXID})
			_ = privateChatPuppet.DefaultIntent().EnsureJoined(portal.MXID)
		}
	}
	return func() {
		portal.backfilling = false
		portal.privateChatBackfillInvitePuppet = nil
		portal.backfillLock.Unlock()
		if privateChatPuppet != nil && privateChatPuppetInvited {
			_, _ = privateChatPuppet.DefaultIntent().LeaveRoom(portal.MXID)
		}
	}
}

func (portal *Portal) disableNotifications(user *User) {
	if !portal.bridge.Config.Bridge.HistoryDisableNotifs {
		return
	}
	puppet := portal.bridge.GetPuppetByCustomMXID(user.MXID)
	if puppet == nil || puppet.customIntent == nil {
		return
	}
	portal.log.Debugfln("Disabling notifications for %s for backfilling", user.MXID)
	ruleID := fmt.Sprintf("net.maunium.silence_while_backfilling.%s", portal.MXID)
	err := puppet.customIntent.PutPushRule("global", pushrules.OverrideRule, ruleID, &mautrix.ReqPutPushRule{
		Actions: []pushrules.PushActionType{pushrules.ActionDontNotify},
		Conditions: []pushrules.PushCondition{{
			Kind:    pushrules.KindEventMatch,
			Key:     "room_id",
			Pattern: string(portal.MXID),
		}},
	})
	if err != nil {
		portal.log.Warnfln("Failed to disable notifications for %s while backfilling: %v", user.MXID, err)
	}
}

func (portal *Portal) enableNotifications(user *User) {
	if !portal.bridge.Config.Bridge.HistoryDisableNotifs {
		return
	}
	puppet := portal.bridge.GetPuppetByCustomMXID(user.MXID)
	if puppet == nil || puppet.customIntent == nil {
		return
	}
	portal.log.Debugfln("Re-enabling notifications for %s after backfilling", user.MXID)
	ruleID := fmt.Sprintf("net.maunium.silence_while_backfilling.%s", portal.MXID)
	err := puppet.customIntent.DeletePushRule("global", pushrules.OverrideRule, ruleID)
	if err != nil {
		portal.log.Warnfln("Failed to re-enable notifications for %s after backfilling: %v", user.MXID, err)
	}
}

func (portal *Portal) FillInitialHistory(user *User) error {
	if portal.bridge.Config.Bridge.InitialHistoryFill == 0 {
		return nil
	}
	endBackfill := portal.beginBackfill()
	defer endBackfill()
	if portal.privateChatBackfillInvitePuppet != nil {
		portal.privateChatBackfillInvitePuppet()
	}

	n := portal.bridge.Config.Bridge.InitialHistoryFill
	portal.log.Infoln("Filling initial history, maximum", n, "messages")
	var messages []interface{}
	before := ""
	fromMe := true
	chunkNum := 1
	for n > 0 {
		count := 50
		if n < count {
			count = n
		}
		portal.log.Debugfln("Fetching chunk %d (%d messages / %d cap) before message %s", chunkNum, count, n, before)
		resp, err := user.Conn.LoadMessagesBefore(portal.Key.JID, before, fromMe, count)
		if err != nil {
			return err
		}
		chunk, ok := resp.Content.([]interface{})
		if !ok || len(chunk) == 0 {
			portal.log.Infoln("Chunk empty, starting handling of loaded messages")
			break
		}

		messages = append(chunk, messages...)

		portal.log.Debugfln("Fetched chunk and received %d messages", len(chunk))

		n -= len(chunk)
		key := chunk[0].(*waProto.WebMessageInfo).GetKey()
		before = key.GetId()
		fromMe = key.GetFromMe()
		if len(before) == 0 {
			portal.log.Infoln("No message ID for first message, starting handling of loaded messages")
			break
		}
	}
	portal.disableNotifications(user)
	portal.handleHistory(user, messages)
	portal.enableNotifications(user)
	portal.log.Infoln("Initial history fill complete")
	return nil
}

func (portal *Portal) handleHistory(user *User, messages []interface{}) {
	portal.log.Infoln("Handling", len(messages), "messages of history")
	for _, rawMessage := range messages {
		message, ok := rawMessage.(*waProto.WebMessageInfo)
		if !ok {
			portal.log.Warnln("Unexpected non-WebMessageInfo item in history response:", rawMessage)
			continue
		}
		data := whatsapp.ParseProtoMessage(message)
		if data == nil || data == whatsapp.ErrMessageTypeNotImplemented {
			st := message.GetMessageStubType()
			// Ignore some types that are known to fail
			if st == waProto.WebMessageInfo_CALL_MISSED_VOICE || st == waProto.WebMessageInfo_CALL_MISSED_VIDEO ||
				st == waProto.WebMessageInfo_CALL_MISSED_GROUP_VOICE || st == waProto.WebMessageInfo_CALL_MISSED_GROUP_VIDEO {
				continue
			}
			portal.log.Warnln("Message", message.GetKey().GetId(), "failed to parse during backfilling")
			continue
		}
		if portal.privateChatBackfillInvitePuppet != nil && message.GetKey().GetFromMe() && portal.IsPrivateChat() {
			portal.privateChatBackfillInvitePuppet()
		}
		portal.handleMessage(PortalMessage{portal.Key.JID, user, data, message.GetMessageTimestamp()})
	}
}

type BridgeInfoSection struct {
	ID          string              `json:"id"`
	DisplayName string              `json:"displayname,omitempty"`
	AvatarURL   id.ContentURIString `json:"avatar_url,omitempty"`
	ExternalURL string              `json:"external_url,omitempty"`
}

type BridgeInfoContent struct {
	BridgeBot id.UserID          `json:"bridgebot"`
	Creator   id.UserID          `json:"creator,omitempty"`
	Protocol  BridgeInfoSection  `json:"protocol"`
	Network   *BridgeInfoSection `json:"network,omitempty"`
	Channel   BridgeInfoSection  `json:"channel"`
}

var (
	StateBridgeInfo         = event.Type{Type: "m.bridge", Class: event.StateEventType}
	StateHalfShotBridgeInfo = event.Type{Type: "uk.half-shot.bridge", Class: event.StateEventType}
)

func (portal *Portal) getBridgeInfo() (string, BridgeInfoContent) {
	bridgeInfo := BridgeInfoContent{
		BridgeBot: portal.bridge.Bot.UserID,
		Creator:   portal.MainIntent().UserID,
		Protocol: BridgeInfoSection{
			ID:          "whatsapp",
			DisplayName: "WhatsApp",
			AvatarURL:   id.ContentURIString(portal.bridge.Config.AppService.Bot.Avatar),
			ExternalURL: "https://www.whatsapp.com/",
		},
		Channel: BridgeInfoSection{
			ID:          portal.Key.JID,
			DisplayName: portal.Name,
			AvatarURL:   portal.AvatarURL.CUString(),
		},
	}
	bridgeInfoStateKey := fmt.Sprintf("net.maunium.whatsapp://whatsapp/%s", portal.Key.JID)
	return bridgeInfoStateKey, bridgeInfo
}

func (portal *Portal) UpdateBridgeInfo() {
	if len(portal.MXID) == 0 {
		portal.log.Debugln("Not updating bridge info: no Matrix room created")
		return
	}
	portal.log.Debugln("Updating bridge info...")
	stateKey, content := portal.getBridgeInfo()
	_, err := portal.MainIntent().SendStateEvent(portal.MXID, StateBridgeInfo, stateKey, content)
	if err != nil {
		portal.log.Warnln("Failed to update m.bridge:", err)
	}
	_, err = portal.MainIntent().SendStateEvent(portal.MXID, StateHalfShotBridgeInfo, stateKey, content)
	if err != nil {
		portal.log.Warnln("Failed to update uk.half-shot.bridge:", err)
	}
}

func (portal *Portal) CreateMatrixRoom(user *User) error {
	portal.roomCreateLock.Lock()
	defer portal.roomCreateLock.Unlock()
	if len(portal.MXID) > 0 {
		return nil
	}

	intent := portal.MainIntent()
	if err := intent.EnsureRegistered(); err != nil {
		return err
	}

	portal.log.Infoln("Creating Matrix room. Info source:", user.MXID)

	var metadata *whatsappExt.GroupInfo
	if portal.IsPrivateChat() {
		puppet := portal.bridge.GetPuppetByJID(portal.Key.JID)
		if portal.bridge.Config.Bridge.PrivateChatPortalMeta {
			portal.Name = puppet.Displayname
			portal.AvatarURL = puppet.AvatarURL
			portal.Avatar = puppet.Avatar
		} else {
			portal.Name = ""
		}
		portal.Topic = "WhatsApp private chat"
	} else if portal.IsStatusBroadcastRoom() {
		portal.Name = "WhatsApp Status Broadcast"
		portal.Topic = "WhatsApp status updates from your contacts"
	} else {
		var err error
		metadata, err = user.Conn.GetGroupMetaData(portal.Key.JID)
		if err == nil && metadata.Status == 0 {
			portal.Name = metadata.Name
			portal.Topic = metadata.Topic
		}
		portal.UpdateAvatar(user, nil, false)
	}

	bridgeInfoStateKey, bridgeInfo := portal.getBridgeInfo()

	initialState := []*event.Event{{
		Type: event.StatePowerLevels,
		Content: event.Content{
			Parsed: portal.GetBasePowerLevels(),
		},
	}, {
		Type:     StateBridgeInfo,
		Content:  event.Content{Parsed: bridgeInfo},
		StateKey: &bridgeInfoStateKey,
	}, {
		// TODO remove this once https://github.com/matrix-org/matrix-doc/pull/2346 is in spec
		Type:     StateHalfShotBridgeInfo,
		Content:  event.Content{Parsed: bridgeInfo},
		StateKey: &bridgeInfoStateKey,
	}}
	if !portal.AvatarURL.IsEmpty() {
		initialState = append(initialState, &event.Event{
			Type: event.StateRoomAvatar,
			Content: event.Content{
				Parsed: event.RoomAvatarEventContent{URL: portal.AvatarURL},
			},
		})
	}

	invite := []id.UserID{user.MXID}
	if user.IsRelaybot {
		invite = portal.bridge.Config.Bridge.Relaybot.InviteUsers
	}

	if portal.bridge.Config.Bridge.Encryption.Default {
		initialState = append(initialState, &event.Event{
			Type: event.StateEncryption,
			Content: event.Content{
				Parsed: event.EncryptionEventContent{Algorithm: id.AlgorithmMegolmV1},
			},
		})
		portal.Encrypted = true
		if portal.IsPrivateChat() {
			invite = append(invite, portal.bridge.Bot.UserID)
		}
	}

	resp, err := intent.CreateRoom(&mautrix.ReqCreateRoom{
		Visibility:   "private",
		Name:         portal.Name,
		Topic:        portal.Topic,
		Invite:       invite,
		Preset:       "private_chat",
		IsDirect:     portal.IsPrivateChat(),
		InitialState: initialState,
	})
	if err != nil {
		return err
	}
	portal.MXID = resp.RoomID
	portal.Update()
	portal.bridge.portalsLock.Lock()
	portal.bridge.portalsByMXID[portal.MXID] = portal
	portal.bridge.portalsLock.Unlock()

	// We set the memberships beforehand to make sure the encryption key exchange in initial backfill knows the users are here.
	for _, user := range invite {
		portal.bridge.StateStore.SetMembership(portal.MXID, user, event.MembershipInvite)
	}

	if metadata != nil {
		portal.SyncParticipants(metadata)
		if metadata.Announce {
			portal.RestrictMessageSending(metadata.Announce)
		}
	} else {
		customPuppet := portal.bridge.GetPuppetByCustomMXID(user.MXID)
		if customPuppet != nil && customPuppet.CustomIntent() != nil {
			_ = customPuppet.CustomIntent().EnsureJoined(portal.MXID)
		}
	}
	user.addPortalToCommunity(portal)
	if portal.IsPrivateChat() {
		puppet := user.bridge.GetPuppetByJID(portal.Key.JID)
		user.addPuppetToCommunity(puppet)

		if portal.bridge.Config.Bridge.Encryption.Default {
			err = portal.bridge.Bot.EnsureJoined(portal.MXID)
			if err != nil {
				portal.log.Errorln("Failed to join created portal with bridge bot for e2be:", err)
			}
		}

		user.UpdateDirectChats(map[id.UserID][]id.RoomID{puppet.MXID: {portal.MXID}})
	}
	err = portal.FillInitialHistory(user)
	if err != nil {
		portal.log.Errorln("Failed to fill history:", err)
	}
	return nil
}

func (portal *Portal) IsPrivateChat() bool {
	if portal.isPrivate == nil {
		val := strings.HasSuffix(portal.Key.JID, whatsappExt.NewUserSuffix)
		portal.isPrivate = &val
	}
	return *portal.isPrivate
}

func (portal *Portal) HasRelaybot() bool {
	if portal.bridge.Relaybot == nil {
		return false
	} else if portal.hasRelaybot == nil {
		val := portal.bridge.Relaybot.IsInPortal(portal.Key)
		portal.hasRelaybot = &val
	}
	return *portal.hasRelaybot
}

func (portal *Portal) IsStatusBroadcastRoom() bool {
	return portal.Key.JID == "status@broadcast"
}

func (portal *Portal) MainIntent() *appservice.IntentAPI {
	if portal.IsPrivateChat() {
		return portal.bridge.GetPuppetByJID(portal.Key.JID).DefaultIntent()
	}
	return portal.bridge.Bot
}

func (portal *Portal) SetReply(content *event.MessageEventContent, info whatsapp.ContextInfo) {
	if len(info.QuotedMessageID) == 0 {
		return
	}
	message := portal.bridge.DB.Message.GetByJID(portal.Key, info.QuotedMessageID)
	if message != nil {
		evt, err := portal.MainIntent().GetEvent(portal.MXID, message.MXID)
		if err != nil {
			portal.log.Warnln("Failed to get reply target:", err)
			return
		}
		if evt.Type == event.EventEncrypted {
			_ = evt.Content.ParseRaw(evt.Type)
			decryptedEvt, err := portal.bridge.Crypto.Decrypt(evt)
			if err != nil {
				portal.log.Warnln("Failed to decrypt reply target:", err)
			} else {
				evt = decryptedEvt
			}
		}
		_ = evt.Content.ParseRaw(evt.Type)
		content.SetReply(evt)
	}
	return
}

func (portal *Portal) HandleMessageRevoke(user *User, message whatsappExt.MessageRevocation) {
	msg := portal.bridge.DB.Message.GetByJID(portal.Key, message.Id)
	if msg == nil {
		return
	}
	var intent *appservice.IntentAPI
	if message.FromMe {
		if portal.IsPrivateChat() {
			intent = portal.bridge.GetPuppetByJID(user.JID).CustomIntent()
		} else {
			intent = portal.bridge.GetPuppetByJID(user.JID).IntentFor(portal)
		}
	} else if len(message.Participant) > 0 {
		intent = portal.bridge.GetPuppetByJID(message.Participant).IntentFor(portal)
	}
	if intent == nil {
		intent = portal.MainIntent()
	}
	_, err := intent.RedactEvent(portal.MXID, msg.MXID)
	if err != nil {
		portal.log.Errorln("Failed to redact %s: %v", msg.JID, err)
		return
	}
	msg.Delete()
}

func (portal *Portal) HandleFakeMessage(_ *User, message FakeMessage) {
	if portal.isRecentlyHandled(message.ID) {
		return
	}

	content := event.MessageEventContent{
		MsgType: event.MsgNotice,
		Body:    message.Text,
	}
	if message.Alert {
		content.MsgType = event.MsgText
	}
	_, err := portal.sendMainIntentMessage(content)
	if err != nil {
		portal.log.Errorfln("Failed to handle fake message %s: %v", message.ID, err)
		return
	}

	portal.recentlyHandledLock.Lock()
	index := portal.recentlyHandledIndex
	portal.recentlyHandledIndex = (portal.recentlyHandledIndex + 1) % recentlyHandledLength
	portal.recentlyHandledLock.Unlock()
	portal.recentlyHandled[index] = message.ID
}

func (portal *Portal) sendMainIntentMessage(content interface{}) (*mautrix.RespSendEvent, error) {
	return portal.sendMessage(portal.MainIntent(), event.EventMessage, content, 0)
}

const MessageSendRetries = 5
const MediaUploadRetries = 5
const BadGatewaySleep = 5 * time.Second

func (portal *Portal) sendMessage(intent *appservice.IntentAPI, eventType event.Type, content interface{}, timestamp int64) (*mautrix.RespSendEvent, error) {
	return portal.sendMessageWithRetry(intent, eventType, content, timestamp, MessageSendRetries)
}

func isGatewayError(err error) bool {
	if err == nil {
		return false
	}
	var httpErr mautrix.HTTPError
	return errors.As(err, &httpErr) && (httpErr.IsStatus(http.StatusBadGateway) || httpErr.IsStatus(http.StatusGatewayTimeout))
}

func (portal *Portal) sendMessageWithRetry(intent *appservice.IntentAPI, eventType event.Type, content interface{}, timestamp int64, retries int) (*mautrix.RespSendEvent, error) {
	for ;;retries-- {
		resp, err := portal.sendMessageDirect(intent, eventType, content, timestamp)
		if retries > 0 && isGatewayError(err) {
			portal.log.Warnfln("Got gateway error trying to send message, retrying in %d seconds", int(BadGatewaySleep.Seconds()))
			time.Sleep(BadGatewaySleep)
		} else {
			return resp, err
		}
	}
}

func (portal *Portal) sendMessageDirect(intent *appservice.IntentAPI, eventType event.Type, content interface{}, timestamp int64) (*mautrix.RespSendEvent, error) {
	wrappedContent := event.Content{Parsed: content}
	if timestamp != 0 && intent.IsCustomPuppet {
		wrappedContent.Raw = map[string]interface{}{
			"net.maunium.whatsapp.puppet": intent.IsCustomPuppet,
		}
	}
	if portal.Encrypted && portal.bridge.Crypto != nil {
		encrypted, err := portal.bridge.Crypto.Encrypt(portal.MXID, eventType, wrappedContent)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt event: %w", err)
		}
		eventType = event.EventEncrypted
		wrappedContent.Parsed = encrypted
	}
	if timestamp == 0 {
		return intent.SendMessageEvent(portal.MXID, eventType, &wrappedContent)
	} else {
		return intent.SendMassagedMessageEvent(portal.MXID, eventType, &wrappedContent, timestamp)
	}
}

func (portal *Portal) HandleTextMessage(source *User, message whatsapp.TextMessage) {
	intent := portal.startHandling(source, message.Info)
	if intent == nil {
		return
	}

	content := &event.MessageEventContent{
		Body:    message.Text,
		MsgType: event.MsgText,
	}

	portal.bridge.Formatter.ParseWhatsApp(content, message.ContextInfo.MentionedJID)
	portal.SetReply(content, message.ContextInfo)

	_, _ = intent.UserTyping(portal.MXID, false, 0)
	resp, err := portal.sendMessage(intent, event.EventMessage, content, int64(message.Info.Timestamp*1000))
	if err != nil {
		portal.log.Errorfln("Failed to handle message %s: %v", message.Info.Id, err)
		return
	}
	portal.finishHandling(source, message.Info.Source, resp.EventID)
}

func (portal *Portal) HandleLocationMessage(source *User, message whatsapp.LocationMessage) {
	intent := portal.startHandling(source, message.Info)
	if intent == nil {
		return
	}

	url := message.Url
	if len(url) == 0 {
		url = fmt.Sprintf("https://maps.google.com/?q=%.5f,%.5f", message.DegreesLatitude, message.DegreesLongitude)
	}
	name := message.Name
	if len(name) == 0 {
		latChar := 'N'
		if message.DegreesLatitude < 0 {
			latChar = 'S'
		}
		longChar := 'E'
		if message.DegreesLongitude < 0 {
			longChar = 'W'
		}
		name = fmt.Sprintf("%.4f° %c %.4f° %c", math.Abs(message.DegreesLatitude), latChar, math.Abs(message.DegreesLongitude), longChar)
	}

	content := &event.MessageEventContent{
		MsgType:       event.MsgLocation,
		Body:          fmt.Sprintf("Location: %s\n%s\n%s", name, message.Address, url),
		Format:        event.FormatHTML,
		FormattedBody: fmt.Sprintf("Location: <a href='%s'>%s</a><br>%s", url, name, message.Address),
		GeoURI:        fmt.Sprintf("geo:%.5f,%.5f", message.DegreesLatitude, message.DegreesLongitude),
	}

	if len(message.JpegThumbnail) > 0 {
		thumbnailMime := http.DetectContentType(message.JpegThumbnail)
		uploadedThumbnail, _ := intent.UploadBytes(message.JpegThumbnail, thumbnailMime)
		if uploadedThumbnail != nil {
			cfg, _, _ := image.DecodeConfig(bytes.NewReader(message.JpegThumbnail))
			content.Info = &event.FileInfo{
				ThumbnailInfo: &event.FileInfo{
					Size:     len(message.JpegThumbnail),
					Width:    cfg.Width,
					Height:   cfg.Height,
					MimeType: thumbnailMime,
				},
				ThumbnailURL: uploadedThumbnail.ContentURI.CUString(),
			}
		}
	}

	portal.SetReply(content, message.ContextInfo)

	_, _ = intent.UserTyping(portal.MXID, false, 0)
	resp, err := portal.sendMessage(intent, event.EventMessage, content, int64(message.Info.Timestamp*1000))
	if err != nil {
		portal.log.Errorfln("Failed to handle message %s: %v", message.Info.Id, err)
		return
	}
	portal.finishHandling(source, message.Info.Source, resp.EventID)
}

func (portal *Portal) HandleContactMessage(source *User, message whatsapp.ContactMessage) {
	intent := portal.startHandling(source, message.Info)
	if intent == nil {
		return
	}

	fileName := fmt.Sprintf("%s.vcf", message.DisplayName)
	data := []byte(message.Vcard)
	mimeType := "text/vcard"
	data, uploadMimeType, file := portal.encryptFile(data, mimeType)

	uploadResp, err := intent.UploadBytesWithName(data, uploadMimeType, fileName)
	if err != nil {
		portal.log.Errorfln("Failed to upload vcard of %s: %v", message.DisplayName, err)
		return
	}

	content := &event.MessageEventContent{
		Body:    fileName,
		MsgType: event.MsgFile,
		File:    file,
		Info: &event.FileInfo{
			MimeType: mimeType,
			Size:     len(message.Vcard),
		},
	}
	if content.File != nil {
		content.File.URL = uploadResp.ContentURI.CUString()
	} else {
		content.URL = uploadResp.ContentURI.CUString()
	}

	portal.SetReply(content, message.ContextInfo)

	_, _ = intent.UserTyping(portal.MXID, false, 0)
	resp, err := portal.sendMessage(intent, event.EventMessage, content, int64(message.Info.Timestamp*1000))
	if err != nil {
		portal.log.Errorfln("Failed to handle message %s: %v", message.Info.Id, err)
		return
	}
	portal.finishHandling(source, message.Info.Source, resp.EventID)
}

func (portal *Portal) sendMediaBridgeFailure(source *User, intent *appservice.IntentAPI, info whatsapp.MessageInfo, bridgeErr error) {
	portal.log.Errorfln("Failed to bridge media for %s: %v", info.Id, bridgeErr)
	resp, err := portal.sendMessage(intent, event.EventMessage, &event.MessageEventContent{
		MsgType: event.MsgNotice,
		Body:    "Failed to bridge media",
	}, int64(info.Timestamp*1000))
	if err != nil {
		portal.log.Errorfln("Failed to send media download error message for %s: %v", info.Id, err)
	} else {
		portal.finishHandling(source, info.Source, resp.EventID)
	}
}

func (portal *Portal) encryptFile(data []byte, mimeType string) ([]byte, string, *event.EncryptedFileInfo) {
	if !portal.Encrypted {
		return data, mimeType, nil
	}

	file := &event.EncryptedFileInfo{
		EncryptedFile: *attachment.NewEncryptedFile(),
		URL:           "",
	}
	return file.Encrypt(data), "application/octet-stream", file
}

func (portal *Portal) tryKickUser(userID id.UserID, intent *appservice.IntentAPI) error {
	_, err := intent.KickUser(portal.MXID, &mautrix.ReqKickUser{UserID: userID})
	if err != nil {
		httpErr, ok := err.(mautrix.HTTPError)
		if ok && httpErr.RespError != nil && httpErr.RespError.ErrCode == "M_FORBIDDEN" {
			_, err = portal.MainIntent().KickUser(portal.MXID, &mautrix.ReqKickUser{UserID: userID})
		}
	}
	return err
}

func (portal *Portal) removeUser(isSameUser bool, kicker *appservice.IntentAPI, target id.UserID, targetIntent *appservice.IntentAPI) {
	if !isSameUser || targetIntent == nil {
		err := portal.tryKickUser(target, kicker)
		if err != nil {
			portal.log.Warnfln("Failed to kick %s from %s: %v", target, portal.MXID, err)
			if targetIntent != nil {
				_, _ = targetIntent.LeaveRoom(portal.MXID)
			}
		}
	} else {
		_, err := targetIntent.LeaveRoom(portal.MXID)
		if err != nil {
			portal.log.Warnfln("Failed to leave portal as %s: %v", target, err)
			_, _ = portal.MainIntent().KickUser(portal.MXID, &mautrix.ReqKickUser{UserID: target})
		}
	}
}

func (portal *Portal) HandleWhatsAppKick(senderJID string, jids []string) {
	sender := portal.bridge.GetPuppetByJID(senderJID)
	senderIntent := sender.IntentFor(portal)
	for _, jid := range jids {
		puppet := portal.bridge.GetPuppetByJID(jid)
		portal.removeUser(puppet.JID == sender.JID, senderIntent, puppet.MXID, puppet.DefaultIntent())

		user := portal.bridge.GetUserByJID(jid)
		if user != nil {
			var customIntent *appservice.IntentAPI
			if puppet.CustomMXID == user.MXID {
				customIntent = puppet.CustomIntent()
			}
			portal.removeUser(puppet.JID == sender.JID, senderIntent, user.MXID, customIntent)
		}
	}
}

func (portal *Portal) HandleWhatsAppInvite(senderJID string, jids []string) {
	senderIntent := portal.MainIntent()
	if senderJID != "unknown" {
		sender := portal.bridge.GetPuppetByJID(senderJID)
		senderIntent = sender.IntentFor(portal)
	}
	for _, jid := range jids {
		puppet := portal.bridge.GetPuppetByJID(jid)
		_, err := senderIntent.InviteUser(portal.MXID, &mautrix.ReqInviteUser{UserID: puppet.MXID})
		if err != nil {
			portal.log.Warnfln("Failed to invite %s as %s: %v", puppet.MXID, senderIntent.UserID, err)
		}
		err = puppet.DefaultIntent().EnsureJoined(portal.MXID)
		if err != nil {
			portal.log.Errorfln("Failed to ensure %s is joined: %v", puppet.MXID, err)
		}
	}
}

type base struct {
	download func() ([]byte, error)
	info     whatsapp.MessageInfo
	context  whatsapp.ContextInfo
	mimeType string
}

type mediaMessage struct {
	base

	thumbnail     []byte
	caption       string
	fileName      string
	length        uint32
	sendAsSticker bool
}

func (portal *Portal) uploadWithRetry(intent *appservice.IntentAPI, data []byte, mimeType string, retries int) (*mautrix.RespMediaUpload, error) {
	for ;;retries-- {
		uploaded, err := intent.UploadBytes(data, mimeType)
		if isGatewayError(err) {
			portal.log.Warnfln("Got gateway error trying to upload media, retrying in %d seconds", int(BadGatewaySleep.Seconds()))
			time.Sleep(BadGatewaySleep)
		} else {
			return uploaded, err
		}
	}
}

func (portal *Portal) HandleMediaMessage(source *User, msg mediaMessage) {
	intent := portal.startHandling(source, msg.info)
	if intent == nil {
		return
	}

	data, err := msg.download()
	if err == whatsapp.ErrMediaDownloadFailedWith404 || err == whatsapp.ErrMediaDownloadFailedWith410 {
		portal.log.Warnfln("Failed to download media for %s: %v. Calling LoadMediaInfo and retrying download...", msg.info.Id, err)
		_, err = source.Conn.LoadMediaInfo(msg.info.RemoteJid, msg.info.Id, msg.info.FromMe)
		if err != nil {
			portal.sendMediaBridgeFailure(source, intent, msg.info, fmt.Errorf("failed to load media info: %w", err))
			return
		}
		data, err = msg.download()
	}
	if err == whatsapp.ErrNoURLPresent {
		portal.log.Debugfln("No URL present error for media message %s, ignoring...", msg.info.Id)
		return
	} else if err != nil {
		portal.sendMediaBridgeFailure(source, intent, msg.info, err)
		return
	}

	var width, height int
	if strings.HasPrefix(msg.mimeType, "image/") {
		cfg, _, _ := image.DecodeConfig(bytes.NewReader(data))
		width, height = cfg.Width, cfg.Height
	}

	data, uploadMimeType, file := portal.encryptFile(data, msg.mimeType)

	uploaded, err := portal.uploadWithRetry(intent, data, uploadMimeType, MediaUploadRetries)
	if err != nil {
		if errors.Is(err, mautrix.MTooLarge) {
			portal.sendMediaBridgeFailure(source, intent, msg.info, errors.New("homeserver rejected too large file"))
		} else if httpErr := err.(mautrix.HTTPError); httpErr.IsStatus(413) {
			portal.sendMediaBridgeFailure(source, intent, msg.info, errors.New("proxy rejected too large file"))
		} else {
			portal.sendMediaBridgeFailure(source, intent, msg.info, fmt.Errorf("failed to upload media: %w", err))
		}
		return
	}

	if msg.fileName == "" {
		mimeClass := strings.Split(msg.mimeType, "/")[0]
		switch mimeClass {
		case "application":
			msg.fileName = "file"
		default:
			msg.fileName = mimeClass
		}

		exts, _ := mime.ExtensionsByType(msg.mimeType)
		if exts != nil && len(exts) > 0 {
			msg.fileName += exts[0]
		}
	}

	content := &event.MessageEventContent{
		Body: msg.fileName,
		File: file,
		Info: &event.FileInfo{
			Size:     len(data),
			MimeType: msg.mimeType,
			Width:    width,
			Height:   height,
			Duration: int(msg.length),
		},
	}
	if content.File != nil {
		content.File.URL = uploaded.ContentURI.CUString()
	} else {
		content.URL = uploaded.ContentURI.CUString()
	}
	portal.SetReply(content, msg.context)

	if msg.thumbnail != nil && portal.bridge.Config.Bridge.WhatsappThumbnail {
		thumbnailMime := http.DetectContentType(msg.thumbnail)
		thumbnailCfg, _, _ := image.DecodeConfig(bytes.NewReader(msg.thumbnail))
		thumbnailSize := len(msg.thumbnail)
		thumbnail, thumbnailUploadMime, thumbnailFile := portal.encryptFile(msg.thumbnail, thumbnailMime)
		uploadedThumbnail, err := intent.UploadBytes(thumbnail, thumbnailUploadMime)
		if err != nil {
			portal.log.Warnfln("Failed to upload thumbnail for %s: %v", msg.info.Id, err)
		} else if uploadedThumbnail != nil {
			if thumbnailFile != nil {
				thumbnailFile.URL = uploadedThumbnail.ContentURI.CUString()
				content.Info.ThumbnailFile = thumbnailFile
			} else {
				content.Info.ThumbnailURL = uploadedThumbnail.ContentURI.CUString()
			}
			content.Info.ThumbnailInfo = &event.FileInfo{
				Size:     thumbnailSize,
				Width:    thumbnailCfg.Width,
				Height:   thumbnailCfg.Height,
				MimeType: thumbnailMime,
			}
		}
	}

	switch strings.ToLower(strings.Split(msg.mimeType, "/")[0]) {
	case "image":
		if !msg.sendAsSticker {
			content.MsgType = event.MsgImage
		}
	case "video":
		content.MsgType = event.MsgVideo
	case "audio":
		content.MsgType = event.MsgAudio
	default:
		content.MsgType = event.MsgFile
	}

	_, _ = intent.UserTyping(portal.MXID, false, 0)
	ts := int64(msg.info.Timestamp * 1000)
	eventType := event.EventMessage
	if msg.sendAsSticker {
		eventType = event.EventSticker
	}
	resp, err := portal.sendMessage(intent, eventType, content, ts)
	if err != nil {
		portal.log.Errorfln("Failed to handle message %s: %v", msg.info.Id, err)
		return
	}

	if len(msg.caption) > 0 {
		captionContent := &event.MessageEventContent{
			Body:    msg.caption,
			MsgType: event.MsgNotice,
		}

		portal.bridge.Formatter.ParseWhatsApp(captionContent, msg.context.MentionedJID)

		_, err := portal.sendMessage(intent, event.EventMessage, captionContent, ts)
		if err != nil {
			portal.log.Warnfln("Failed to handle caption of message %s: %v", msg.info.Id, err)
		}
		// TODO store caption mxid?
	}

	portal.finishHandling(source, msg.info.Source, resp.EventID)
}

func makeMessageID() *string {
	b := make([]byte, 10)
	rand.Read(b)
	str := strings.ToUpper(hex.EncodeToString(b))
	return &str
}

func (portal *Portal) downloadThumbnail(content *event.MessageEventContent, id id.EventID) []byte {
	if len(content.GetInfo().ThumbnailURL) == 0 {
		return nil
	}
	mxc, err := content.GetInfo().ThumbnailURL.Parse()
	if err != nil {
		portal.log.Errorln("Malformed thumbnail URL in %s: %v", id, err)
	}
	thumbnail, err := portal.MainIntent().DownloadBytes(mxc)
	if err != nil {
		portal.log.Errorln("Failed to download thumbnail in %s: %v", id, err)
		return nil
	}
	thumbnailType := http.DetectContentType(thumbnail)
	var img image.Image
	switch thumbnailType {
	case "image/png":
		img, err = png.Decode(bytes.NewReader(thumbnail))
	case "image/gif":
		img, err = gif.Decode(bytes.NewReader(thumbnail))
	case "image/jpeg":
		return thumbnail
	default:
		return nil
	}
	var buf bytes.Buffer
	err = jpeg.Encode(&buf, img, &jpeg.Options{
		Quality: jpeg.DefaultQuality,
	})
	if err != nil {
		portal.log.Errorln("Failed to re-encode thumbnail in %s: %v", id, err)
		return nil
	}
	return buf.Bytes()
}

func (portal *Portal) convertGifToVideo(gif []byte) ([]byte, error) {
	dir, err := ioutil.TempDir("", "gif-convert-*")
	if err != nil {
		return nil, fmt.Errorf("failed to make temp dir: %w", err)
	}
	defer os.RemoveAll(dir)

	inputFile, err := os.OpenFile(filepath.Join(dir, "input.gif"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed open input file: %w", err)
	}
	_, err = inputFile.Write(gif)
	if err != nil {
		_ = inputFile.Close()
		return nil, fmt.Errorf("failed to write gif to input file: %w", err)
	}
	_ = inputFile.Close()

	outputFileName := filepath.Join(dir, "output.mp4")
	cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "warning",
		"-f", "gif", "-i", inputFile.Name(),
		"-pix_fmt", "yuv420p", "-c:v", "libx264", "-movflags", "+faststart",
		"-filter:v", "crop='floor(in_w/2)*2:floor(in_h/2)*2'",
		outputFileName)
	vcLog := portal.log.Sub("VideoConverter").WithDefaultLevel(log.LevelWarn)
	cmd.Stdout = vcLog
	cmd.Stderr = vcLog

	err = cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run ffmpeg: %w", err)
	}
	outputFile, err := os.OpenFile(filepath.Join(dir, "output.mp4"), os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open output file: %w", err)
	}
	defer func() {
		_ = outputFile.Close()
		_ = os.Remove(outputFile.Name())
	}()
	mp4, err := ioutil.ReadAll(outputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read mp4 from output file: %w", err)
	}
	return mp4, nil
}

var (
	errFailedPreprocessMatrixMedia = errors.New("failed to process media")
	errFailedUpload = errors.New("could not upload media")
)

func (portal *Portal) preprocessMatrixMedia(sender *User, relaybotFormatted bool, content *event.MessageEventContent, eventID id.EventID, mediaType whatsapp.MediaType) (*MediaUpload, error) {
	var caption string
	var mentionedJIDs []types.WhatsAppID
	if relaybotFormatted {
		caption, mentionedJIDs = portal.bridge.Formatter.ParseMatrix(content.FormattedBody)
	}

	var file *event.EncryptedFileInfo
	rawMXC := content.URL
	if content.File != nil {
		file = content.File
		rawMXC = file.URL
	}
	mxc, err := rawMXC.Parse()
	if err != nil {
		portal.log.Errorln("Malformed content URL in %s: %v", eventID, err)
		return nil, errFailedPreprocessMatrixMedia
	}
	data, err := portal.MainIntent().DownloadBytes(mxc)
	if err != nil {
		portal.log.Errorfln("Failed to download media in %s: %v", eventID, err)
		return nil, errFailedPreprocessMatrixMedia
	}

	if file != nil {
		data, err = file.Decrypt(data)
		if err != nil {
			portal.log.Errorfln("Failed to decrypt media in %s: %v", eventID, err)
			return nil, errFailedPreprocessMatrixMedia
		}
	}

	if mediaType == whatsapp.MediaVideo && content.GetInfo().MimeType == "image/gif" {
		data, err = portal.convertGifToVideo(data)
		if err != nil {
			portal.log.Errorfln("Failed to convert gif to mp4 in %s: %v", eventID, err)
			return nil, errFailedPreprocessMatrixMedia
		}
		content.Info.MimeType = "video/mp4"
	}

	url, mediaKey, fileEncSHA256, fileSHA256, fileLength, err := sender.Conn.Upload(bytes.NewReader(data), mediaType)
	if err != nil {
		portal.log.Errorfln("Failed to upload media in %s: %v", eventID, err)
		return nil, errFailedUpload
	}

	return &MediaUpload{
		Caption:       caption,
		MentionedJIDs: mentionedJIDs,
		URL:           url,
		MediaKey:      mediaKey,
		FileEncSHA256: fileEncSHA256,
		FileSHA256:    fileSHA256,
		FileLength:    fileLength,
		Thumbnail:     portal.downloadThumbnail(content, eventID),
	}, nil
}

type MediaUpload struct {
	Caption       string
	MentionedJIDs []types.WhatsAppID
	URL           string
	MediaKey      []byte
	FileEncSHA256 []byte
	FileSHA256    []byte
	FileLength    uint64
	Thumbnail     []byte
}

func (portal *Portal) sendMatrixConnectionError(sender *User, eventID id.EventID) bool {
	if !sender.HasSession() {
		portal.log.Debugln("Ignoring event", eventID, "from", sender.MXID, "as user has no session")
		return true
	} else if !sender.IsConnected() {
		portal.log.Debugln("Ignoring event", eventID, "from", sender.MXID, "as user is not connected")
		inRoom := ""
		if portal.IsPrivateChat() {
			inRoom = " in your management room"
		}
		reconnect := fmt.Sprintf("Use `%s reconnect`%s to reconnect.", portal.bridge.Config.Bridge.CommandPrefix, inRoom)
		if sender.IsLoginInProgress() {
			reconnect = "You have a login attempt in progress, please wait."
		}
		msg := format.RenderMarkdown("\u26a0 You are not connected to WhatsApp, so your message was not bridged. "+reconnect, true, false)
		msg.MsgType = event.MsgNotice
		_, err := portal.sendMainIntentMessage(msg)
		if err != nil {
			portal.log.Errorln("Failed to send bridging failure message:", err)
		}
		return true
	}
	return false
}

func (portal *Portal) addRelaybotFormat(sender *User, content *event.MessageEventContent) bool {
	member := portal.MainIntent().Member(portal.MXID, sender.MXID)
	if len(member.Displayname) == 0 {
		member.Displayname = string(sender.MXID)
	}

	if content.Format != event.FormatHTML {
		content.FormattedBody = strings.Replace(html.EscapeString(content.Body), "\n", "<br/>", -1)
		content.Format = event.FormatHTML
	}
	data, err := portal.bridge.Config.Bridge.Relaybot.FormatMessage(content, sender.MXID, member)
	if err != nil {
		portal.log.Errorln("Failed to apply relaybot format:", err)
	}
	content.FormattedBody = data
	return true
}

func (portal *Portal) convertMatrixMessage(sender *User, evt *event.Event) (*waProto.WebMessageInfo, *User, error) {
	content, ok := evt.Content.Parsed.(*event.MessageEventContent)
	if !ok {
		portal.log.Debugfln("Failed to handle event %s: unexpected parsed content type %T", evt.ID, evt.Content.Parsed)
		return nil, sender, nil
	}

	ts := uint64(evt.Timestamp / 1000)
	status := waProto.WebMessageInfo_ERROR
	fromMe := true
	info := &waProto.WebMessageInfo{
		Key: &waProto.MessageKey{
			FromMe:    &fromMe,
			Id:        makeMessageID(),
			RemoteJid: &portal.Key.JID,
		},
		MessageTimestamp: &ts,
		Message:          &waProto.Message{},
		Status:           &status,
	}
	ctxInfo := &waProto.ContextInfo{}
	replyToID := content.GetReplyTo()
	if len(replyToID) > 0 {
		content.RemoveReplyFallback()
		msg := portal.bridge.DB.Message.GetByMXID(replyToID)
		if msg != nil && msg.Content != nil {
			ctxInfo.StanzaId = &msg.JID
			ctxInfo.Participant = &msg.Sender
			ctxInfo.QuotedMessage = msg.Content
		}
	}
	relaybotFormatted := false
	if sender.NeedsRelaybot(portal) {
		if !portal.HasRelaybot() {
			if sender.HasSession() {
				portal.log.Debugln("Database says", sender.MXID, "not in chat and no relaybot, but trying to send anyway")
			} else {
				portal.log.Debugln("Ignoring message from", sender.MXID, "in chat with no relaybot")
				return nil, sender, nil
			}
		} else {
			relaybotFormatted = portal.addRelaybotFormat(sender, content)
			sender = portal.bridge.Relaybot
		}
	}
	if evt.Type == event.EventSticker {
		content.MsgType = event.MsgImage
	} else if content.MsgType == event.MsgImage && content.GetInfo().MimeType == "image/gif" {
		content.MsgType = event.MsgVideo
	}

	switch content.MsgType {
	case event.MsgText, event.MsgEmote, event.MsgNotice:
		text := content.Body
		if content.Format == event.FormatHTML {
			text, ctxInfo.MentionedJid = portal.bridge.Formatter.ParseMatrix(content.FormattedBody)
		}
		if content.MsgType == event.MsgEmote && !relaybotFormatted {
			text = "/me " + text
		}
		if ctxInfo.StanzaId != nil || ctxInfo.MentionedJid != nil {
			info.Message.ExtendedTextMessage = &waProto.ExtendedTextMessage{
				Text:        &text,
				ContextInfo: ctxInfo,
			}
		} else {
			info.Message.Conversation = &text
		}
	case event.MsgImage:
		media, err := portal.preprocessMatrixMedia(sender, relaybotFormatted, content, evt.ID, whatsapp.MediaImage)
		if media == nil {
			return nil, sender, err
		}
		ctxInfo.MentionedJid = media.MentionedJIDs
		info.Message.ImageMessage = &waProto.ImageMessage{
			ContextInfo:   ctxInfo,
			Caption:       &media.Caption,
			JpegThumbnail: media.Thumbnail,
			Url:           &media.URL,
			MediaKey:      media.MediaKey,
			Mimetype:      &content.GetInfo().MimeType,
			FileEncSha256: media.FileEncSHA256,
			FileSha256:    media.FileSHA256,
			FileLength:    &media.FileLength,
		}
	case event.MsgVideo:
		gifPlayback := content.GetInfo().MimeType == "image/gif"
		media, err := portal.preprocessMatrixMedia(sender, relaybotFormatted, content, evt.ID, whatsapp.MediaVideo)
		if media == nil {
			return nil, sender, err
		}
		duration := uint32(content.GetInfo().Duration)
		ctxInfo.MentionedJid = media.MentionedJIDs
		info.Message.VideoMessage = &waProto.VideoMessage{
			ContextInfo:   ctxInfo,
			Caption:       &media.Caption,
			JpegThumbnail: media.Thumbnail,
			Url:           &media.URL,
			MediaKey:      media.MediaKey,
			Mimetype:      &content.GetInfo().MimeType,
			GifPlayback:   &gifPlayback,
			Seconds:       &duration,
			FileEncSha256: media.FileEncSHA256,
			FileSha256:    media.FileSHA256,
			FileLength:    &media.FileLength,
		}
	case event.MsgAudio:
		media, err := portal.preprocessMatrixMedia(sender, relaybotFormatted, content, evt.ID, whatsapp.MediaAudio)
		if media == nil {
			return nil, sender, err
		}
		duration := uint32(content.GetInfo().Duration)
		info.Message.AudioMessage = &waProto.AudioMessage{
			ContextInfo:   ctxInfo,
			Url:           &media.URL,
			MediaKey:      media.MediaKey,
			Mimetype:      &content.GetInfo().MimeType,
			Seconds:       &duration,
			FileEncSha256: media.FileEncSHA256,
			FileSha256:    media.FileSHA256,
			FileLength:    &media.FileLength,
		}
	case event.MsgFile:
		media, err := portal.preprocessMatrixMedia(sender, relaybotFormatted, content, evt.ID, whatsapp.MediaDocument)
		if media == nil {
			return nil, sender, err
		}
		info.Message.DocumentMessage = &waProto.DocumentMessage{
			ContextInfo:   ctxInfo,
			Url:           &media.URL,
			Title:         &content.Body,
			FileName:      &content.Body,
			MediaKey:      media.MediaKey,
			Mimetype:      &content.GetInfo().MimeType,
			FileEncSha256: media.FileEncSHA256,
			FileSha256:    media.FileSHA256,
			FileLength:    &media.FileLength,
		}
	default:
		portal.log.Debugln("Unhandled Matrix event %s: unknown msgtype %s", evt.ID, content.MsgType)
		return nil, sender, nil
	}
	return info, sender, nil
}

func (portal *Portal) wasMessageSent(sender *User, id string) bool {
	_, err := sender.Conn.LoadMessagesAfter(portal.Key.JID, id, true, 0)
	if err != nil {
		if err != whatsapp.ErrServerRespondedWith404 {
			portal.log.Warnfln("Failed to check if message was bridged without response: %v", err)
		}
		return false
	}
	return true
}

func (portal *Portal) sendErrorMessage(message string) id.EventID {
	resp, err := portal.sendMainIntentMessage(event.MessageEventContent{
		MsgType: event.MsgNotice,
		Body:    fmt.Sprintf("\u26a0 Your message may not have been bridged: %v", message),
	})
	if err != nil {
		portal.log.Warnfln("Failed to send bridging error message:", err)
		return ""
	}
	return resp.EventID
}

func (portal *Portal) sendDeliveryReceipt(eventID id.EventID) {
	if portal.bridge.Config.Bridge.DeliveryReceipts {
		err := portal.bridge.Bot.MarkRead(portal.MXID, eventID)
		if err != nil {
			portal.log.Debugfln("Failed to send delivery receipt for %s: %v", eventID, err)
		}
	}
}

var (
	timeout = errors.New("message sending timed out")
	errPossiblyTooLarge = errors.New("could not upload file to WhatsApp, could it be that the file you're trying to send is too large?")
)

func (portal *Portal) HandleMatrixMessage(sender *User, evt *event.Event) {
	if !portal.HasRelaybot() && (
		(portal.IsPrivateChat() && sender.JID != portal.Key.Receiver) ||
			portal.sendMatrixConnectionError(sender, evt.ID)) {
		return
	}
	portal.log.Debugfln("Received event %s", evt.ID)

	info, sender, err := portal.convertMatrixMessage(sender, evt)
	if err != nil {
		if err == errFailedUpload {
			err = errPossiblyTooLarge
		}
		portal.sendErrorMessage(err)
		return
	}
	if info == nil {
		return
	}
	portal.markHandled(sender, info, evt.ID)
	portal.log.Debugln("Sending event", evt.ID, "to WhatsApp", info.Key.GetId())
	portal.sendRaw(sender, evt, info, false)
}

func (portal *Portal) sendRaw(sender *User, evt *event.Event, info *waProto.WebMessageInfo, isRetry bool) {
	errChan := make(chan error, 1)
	go sender.Conn.SendRaw(info, errChan)

	var errorEventID id.EventID
	select {
	case err = <-errChan:
		var statusResp whatsapp.StatusResponse
		if !isRetry && errors.As(err, &statusResp) && statusResp.Status == 599 {
			portal.log.Debugfln("599 status response sending %s to WhatsApp (%+v), retrying...", evt.ID, statusResp)
			errorEventID = portal.sendErrorMessage(fmt.Sprintf("%v. The bridge will retry in 5 seconds.", err))
			time.Sleep(5 * time.Second)
			portal.sendRaw(sender, evt, info, true)
		}
	case <-time.After(time.Duration(portal.bridge.Config.Bridge.ConnectionTimeout) * time.Second):
		if portal.bridge.Config.Bridge.FetchMessageOnTimeout && portal.wasMessageSent(sender, info.Key.GetId()) {
			portal.log.Debugln("Matrix event %s was bridged, but response didn't arrive within timeout")
			portal.sendDeliveryReceipt(evt.ID)
		} else {
			portal.log.Warnfln("Response when bridging Matrix event %s is taking long to arrive", evt.ID)
			errorEventID = portal.sendErrorMessage(timeout.Error())
		}
		err = <-errChan
	}
	if err != nil {
		portal.log.Errorfln("Error handling Matrix event %s: %v", evt.ID, err)
		var statusResp whatsapp.StatusResponse
		if errors.As(err, &statusResp) && statusResp.Status == 599 {
			portal.log.Debugfln("599 status response data: %+v", statusResp)
		}
		portal.sendErrorMessage(err.Error())
	} else {
		portal.log.Debugfln("Handled Matrix event %s", evt.ID)
		portal.sendDeliveryReceipt(evt.ID)
	}
	if errorEventID != "" {
		_, err = portal.MainIntent().RedactEvent(portal.MXID, errorEventID)
		if err != nil {
			portal.log.Warnfln("Failed to redact timeout warning message %s: %v", errorEventID, err)
		}
	}
}

func (portal *Portal) HandleMatrixRedaction(sender *User, evt *event.Event) {
	if portal.IsPrivateChat() && sender.JID != portal.Key.Receiver {
		return
	}

	msg := portal.bridge.DB.Message.GetByMXID(evt.Redacts)
	if msg == nil || msg.Sender != sender.JID {
		return
	}

	ts := uint64(evt.Timestamp / 1000)
	status := waProto.WebMessageInfo_PENDING
	protoMsgType := waProto.ProtocolMessage_REVOKE
	fromMe := true
	info := &waProto.WebMessageInfo{
		Key: &waProto.MessageKey{
			FromMe:    &fromMe,
			Id:        makeMessageID(),
			RemoteJid: &portal.Key.JID,
		},
		MessageTimestamp: &ts,
		Message: &waProto.Message{
			ProtocolMessage: &waProto.ProtocolMessage{
				Type: &protoMsgType,
				Key: &waProto.MessageKey{
					FromMe:    &fromMe,
					Id:        &msg.JID,
					RemoteJid: &portal.Key.JID,
				},
			},
		},
		Status: &status,
	}
	errChan := make(chan error, 1)
	go sender.Conn.SendRaw(info, errChan)

	var err error
	select {
	case err = <-errChan:
	case <-time.After(time.Duration(portal.bridge.Config.Bridge.ConnectionTimeout) * time.Second):
		portal.log.Warnfln("Response when bridging Matrix redaction %s is taking long to arrive", evt.ID)
		err = <-errChan
	}
	if err != nil {
		portal.log.Errorfln("Error handling Matrix redaction %s: %v", evt.ID, err)
	} else {
		portal.log.Debugln("Handled Matrix redaction %s of %s", evt.ID, evt.Redacts)
		portal.sendDeliveryReceipt(evt.ID)
	}
}

func (portal *Portal) Delete() {
	portal.Portal.Delete()
	portal.bridge.portalsLock.Lock()
	delete(portal.bridge.portalsByJID, portal.Key)
	if len(portal.MXID) > 0 {
		delete(portal.bridge.portalsByMXID, portal.MXID)
	}
	portal.bridge.portalsLock.Unlock()
}

func (portal *Portal) GetMatrixUsers() ([]id.UserID, error) {
	members, err := portal.MainIntent().JoinedMembers(portal.MXID)
	if err != nil {
		return nil, fmt.Errorf("failed to get member list: %w", err)
	}
	var users []id.UserID
	for userID := range members.Joined {
		_, isPuppet := portal.bridge.ParsePuppetMXID(userID)
		if !isPuppet && userID != portal.bridge.Bot.UserID {
			users = append(users, userID)
		}
	}
	return users, nil
}

func (portal *Portal) CleanupIfEmpty() {
	users, err := portal.GetMatrixUsers()
	if err != nil {
		portal.log.Errorfln("Failed to get Matrix user list to determine if portal needs to be cleaned up: %v", err)
		return
	}

	if len(users) == 0 {
		portal.log.Infoln("Room seems to be empty, cleaning up...")
		portal.Delete()
		portal.Cleanup(false)
	}
}

func (portal *Portal) Cleanup(puppetsOnly bool) {
	if len(portal.MXID) == 0 {
		return
	}
	if portal.IsPrivateChat() {
		_, err := portal.MainIntent().LeaveRoom(portal.MXID)
		if err != nil {
			portal.log.Warnln("Failed to leave private chat portal with main intent:", err)
		}
		return
	}
	intent := portal.MainIntent()
	members, err := intent.JoinedMembers(portal.MXID)
	if err != nil {
		portal.log.Errorln("Failed to get portal members for cleanup:", err)
		return
	}
	for member := range members.Joined {
		if member == intent.UserID {
			continue
		}
		puppet := portal.bridge.GetPuppetByMXID(member)
		if puppet != nil {
			_, err = puppet.DefaultIntent().LeaveRoom(portal.MXID)
			if err != nil {
				portal.log.Errorln("Error leaving as puppet while cleaning up portal:", err)
			}
		} else if !puppetsOnly {
			_, err = intent.KickUser(portal.MXID, &mautrix.ReqKickUser{UserID: member, Reason: "Deleting portal"})
			if err != nil {
				portal.log.Errorln("Error kicking user while cleaning up portal:", err)
			}
		}
	}
	_, err = intent.LeaveRoom(portal.MXID)
	if err != nil {
		portal.log.Errorln("Error leaving with main intent while cleaning up portal:", err)
	}
}

func (portal *Portal) HandleMatrixLeave(sender *User) {
	if portal.IsPrivateChat() {
		portal.log.Debugln("User left private chat portal, cleaning up and deleting...")
		portal.Delete()
		portal.Cleanup(false)
		return
	} else {
		// TODO should we somehow deduplicate this call if this leave was sent by the bridge?
		resp, err := sender.Conn.LeaveGroup(portal.Key.JID)
		if err != nil {
			portal.log.Errorfln("Failed to leave group as %s: %v", sender.MXID, err)
			return
		}
		portal.log.Infoln("Leave response:", <-resp)
		portal.CleanupIfEmpty()
	}
}

func (portal *Portal) HandleMatrixKick(sender *User, evt *event.Event) {
	puppet := portal.bridge.GetPuppetByMXID(id.UserID(evt.GetStateKey()))
	if puppet != nil {
		resp, err := sender.Conn.RemoveMember(portal.Key.JID, []string{puppet.JID})
		if err != nil {
			portal.log.Errorfln("Failed to kick %s from group as %s: %v", puppet.JID, sender.MXID, err)
			return
		}
		portal.log.Infoln("Kick %s response: %s", puppet.JID, <-resp)
	}
}

func (portal *Portal) HandleMatrixInvite(sender *User, evt *event.Event) {
	puppet := portal.bridge.GetPuppetByMXID(id.UserID(evt.GetStateKey()))
	if puppet != nil {
		resp, err := sender.Conn.AddMember(portal.Key.JID, []string{puppet.JID})
		if err != nil {
			portal.log.Errorfln("Failed to add %s to group as %s: %v", puppet.JID, sender.MXID, err)
			return
		}
		portal.log.Infoln("Add %s response: %s", puppet.JID, <-resp)
	}
}
