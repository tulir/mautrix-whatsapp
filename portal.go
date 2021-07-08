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
	"sync/atomic"
	"time"

	"github.com/Rhymen/go-whatsapp"
	waProto "github.com/Rhymen/go-whatsapp/binary/proto"

	log "maunium.net/go/maulogger/v2"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/crypto/attachment"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"
	"maunium.net/go/mautrix/pushrules"

	"maunium.net/go/mautrix-whatsapp/database"
)

const StatusBroadcastTopic = "WhatsApp status updates from your contacts"
const StatusBroadcastName = "WhatsApp Status Broadcast"
const BroadcastTopic = "WhatsApp broadcast list"
const UnnamedBroadcastName = "Unnamed broadcast list"
const PrivateChatTopic = "WhatsApp private chat"

var ErrStatusBroadcastDisabled = errors.New("status bridging is disabled")

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

func (bridge *Bridge) GetAllPortalsByJID(jid whatsapp.JID) []*Portal {
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

		recentlyHandled: [recentlyHandledLength]whatsapp.MessageID{},

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

		recentlyHandled: [recentlyHandledLength]whatsapp.MessageID{},

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
	encryptLock    sync.Mutex

	recentlyHandled      [recentlyHandledLength]whatsapp.MessageID
	recentlyHandledLock  sync.Mutex
	recentlyHandledIndex uint8

	backfillLock  sync.Mutex
	backfilling   bool
	lastMessageTs uint64

	privateChatBackfillInvitePuppet func()

	messages chan PortalMessage

	isPrivate   *bool
	isBroadcast *bool
	hasRelaybot *bool
}

const MaxMessageAgeToCreatePortal = 5 * 60 // 5 minutes

func (portal *Portal) syncDoublePuppetDetailsAfterCreate(source *User) {
	doublePuppet := portal.bridge.GetPuppetByCustomMXID(source.MXID)
	if doublePuppet == nil {
		return
	}
	source.Conn.Store.ChatsLock.RLock()
	chat, ok := source.Conn.Store.Chats[portal.Key.JID]
	source.Conn.Store.ChatsLock.RUnlock()
	if !ok {
		portal.log.Debugln("Not syncing chat mute/tags with %s: chat info not found", source.MXID)
		return
	}
	source.syncChatDoublePuppetDetails(doublePuppet, Chat{
		Chat:   chat,
		Portal: portal,
	}, true)
}

func (portal *Portal) handleMessageLoop() {
	for msg := range portal.messages {
		if len(portal.MXID) == 0 {
			if msg.timestamp+MaxMessageAgeToCreatePortal < uint64(time.Now().Unix()) {
				portal.log.Debugln("Not creating portal room for incoming message: message is too old")
				continue
			} else if !portal.shouldCreateRoom(msg) {
				portal.log.Debugln("Not creating portal room for incoming message: message is not a chat message")
				continue
			}
			portal.log.Debugln("Creating Matrix room from incoming message")
			err := portal.CreateMatrixRoom(msg.source)
			if err != nil {
				portal.log.Errorln("Failed to create portal room:", err)
				continue
			}
			portal.syncDoublePuppetDetailsAfterCreate(msg.source)
		}
		portal.backfillLock.Lock()
		portal.handleMessage(msg, false)
		portal.backfillLock.Unlock()
	}
}

func (portal *Portal) shouldCreateRoom(msg PortalMessage) bool {
	stubMsg, ok := msg.data.(whatsapp.StubMessage)
	if ok {
		// This could be more specific: if someone else was added, we might not care,
		// but if the local user was added, we definitely care.
		return stubMsg.Type == waProto.WebMessageInfo_GROUP_PARTICIPANT_ADD || stubMsg.Type == waProto.WebMessageInfo_GROUP_PARTICIPANT_INVITE
	}
	return true
}

func (portal *Portal) handleMessage(msg PortalMessage, isBackfill bool) {
	if len(portal.MXID) == 0 {
		portal.log.Warnln("handleMessage called even though portal.MXID is empty")
		return
	}
	var triedToHandle bool
	var trackMessageCallback func()
	dataType := reflect.TypeOf(msg.data)
	if !isBackfill {
		trackMessageCallback = portal.bridge.Metrics.TrackWhatsAppMessage(msg.timestamp, dataType.Name())
	}
	switch data := msg.data.(type) {
	case whatsapp.TextMessage:
		triedToHandle = portal.HandleTextMessage(msg.source, data)
	case whatsapp.ImageMessage:
		triedToHandle = portal.HandleMediaMessage(msg.source, mediaMessage{
			base:      base{data.Download, data.Info, data.ContextInfo, data.Type},
			thumbnail: data.Thumbnail,
			caption:   data.Caption,
		})
	case whatsapp.StickerMessage:
		triedToHandle = portal.HandleMediaMessage(msg.source, mediaMessage{
			base:          base{data.Download, data.Info, data.ContextInfo, data.Type},
			sendAsSticker: true,
		})
	case whatsapp.VideoMessage:
		triedToHandle = portal.HandleMediaMessage(msg.source, mediaMessage{
			base:      base{data.Download, data.Info, data.ContextInfo, data.Type},
			thumbnail: data.Thumbnail,
			caption:   data.Caption,
			length:    data.Length * 1000,
		})
	case whatsapp.AudioMessage:
		triedToHandle = portal.HandleMediaMessage(msg.source, mediaMessage{
			base:   base{data.Download, data.Info, data.ContextInfo, data.Type},
			length: data.Length * 1000,
		})
	case whatsapp.DocumentMessage:
		fileName := data.FileName
		if len(fileName) == 0 {
			fileName = data.Title
		}
		triedToHandle = portal.HandleMediaMessage(msg.source, mediaMessage{
			base:      base{data.Download, data.Info, data.ContextInfo, data.Type},
			thumbnail: data.Thumbnail,
			fileName:  fileName,
		})
	case whatsapp.ContactMessage:
		triedToHandle = portal.HandleContactMessage(msg.source, data)
	case whatsapp.LocationMessage:
		triedToHandle = portal.HandleLocationMessage(msg.source, data)
	case whatsapp.StubMessage:
		triedToHandle = portal.HandleStubMessage(msg.source, data, isBackfill)
	case whatsapp.MessageRevocation:
		triedToHandle = portal.HandleMessageRevoke(msg.source, data)
	case FakeMessage:
		triedToHandle = portal.HandleFakeMessage(msg.source, data)
	default:
		portal.log.Warnln("Unknown message type:", dataType)
	}
	if triedToHandle && trackMessageCallback != nil {
		trackMessageCallback()
	}
}

func (portal *Portal) isRecentlyHandled(id whatsapp.MessageID) bool {
	start := portal.recentlyHandledIndex
	for i := start; i != start; i = (i - 1) % recentlyHandledLength {
		if portal.recentlyHandled[i] == id {
			return true
		}
	}
	return false
}

func (portal *Portal) isDuplicate(id whatsapp.MessageID) bool {
	msg := portal.bridge.DB.Message.GetByJID(portal.Key, id)
	if msg != nil {
		return true
	}
	return false
}

func init() {
	gob.Register(&waProto.Message{})
}

func (portal *Portal) markHandled(source *User, message *waProto.WebMessageInfo, mxid id.EventID, isSent bool) *database.Message {
	msg := portal.bridge.DB.Message.New()
	msg.Chat = portal.Key
	msg.JID = message.GetKey().GetId()
	msg.MXID = mxid
	msg.Timestamp = int64(message.GetMessageTimestamp())
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
	msg.Sent = isSent
	msg.Insert()

	portal.recentlyHandledLock.Lock()
	index := portal.recentlyHandledIndex
	portal.recentlyHandledIndex = (portal.recentlyHandledIndex + 1) % recentlyHandledLength
	portal.recentlyHandledLock.Unlock()
	portal.recentlyHandled[index] = msg.JID
	return msg
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
	puppet := portal.bridge.GetPuppetByJID(info.SenderJid)
	puppet.SyncContactIfNecessary(user)
	return puppet.IntentFor(portal)
}

func (portal *Portal) startHandling(source *User, info whatsapp.MessageInfo, msgType string) *appservice.IntentAPI {
	// TODO these should all be trace logs
	if portal.lastMessageTs == 0 {
		portal.log.Debugln("Fetching last message from database to get its timestamp")
		lastMessage := portal.bridge.DB.Message.GetLastInChat(portal.Key)
		if lastMessage != nil {
			atomic.CompareAndSwapUint64(&portal.lastMessageTs, 0, uint64(lastMessage.Timestamp))
		}
	}

	// If there are messages slightly older than the last message, it's possible the order is just wrong,
	// so don't short-circuit and check the database for duplicates.
	const timestampIgnoreFuzziness = 5 * 60
	if portal.lastMessageTs > info.Timestamp+timestampIgnoreFuzziness {
		portal.log.Debugfln("Not handling %s (%s): message is >5 minutes older (%d) than last bridge message (%d)", info.Id, msgType, info.Timestamp, portal.lastMessageTs)
	} else if portal.isRecentlyHandled(info.Id) {
		portal.log.Debugfln("Not handling %s (%s): message was recently handled", info.Id, msgType)
	} else if portal.isDuplicate(info.Id) {
		portal.log.Debugfln("Not handling %s (%s): message is duplicate", info.Id, msgType)
	} else {
		portal.lastMessageTs = info.Timestamp
		intent := portal.getMessageIntent(source, info)
		if intent != nil {
			portal.log.Debugfln("Starting handling of %s (%s, ts: %d)", info.Id, msgType, info.Timestamp)
		} else {
			portal.log.Debugfln("Not handling %s (%s): sender is not known", info.Id, msgType)
		}
		return intent
	}
	return nil
}

func (portal *Portal) finishHandling(source *User, message *waProto.WebMessageInfo, mxid id.EventID) {
	portal.markHandled(source, message, mxid, true)
	portal.sendDeliveryReceipt(mxid)
	portal.log.Debugln("Handled message", message.GetKey().GetId(), "->", mxid)
}

func (portal *Portal) kickExtraUsers(participantMap map[whatsapp.JID]bool) {
	members, err := portal.MainIntent().JoinedMembers(portal.MXID)
	if err != nil {
		portal.log.Warnln("Failed to get member list:", err)
	} else {
		for member := range members.Joined {
			jid, ok := portal.bridge.ParsePuppetMXID(member)
			if ok {
				_, shouldBePresent := participantMap[jid]
				if !shouldBePresent {
					_, err = portal.MainIntent().KickUser(portal.MXID, &mautrix.ReqKickUser{
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

func (portal *Portal) SyncBroadcastRecipients(source *User, metadata *whatsapp.BroadcastListInfo) {
	participantMap := make(map[whatsapp.JID]bool)
	for _, recipient := range metadata.Recipients {
		participantMap[recipient.JID] = true

		puppet := portal.bridge.GetPuppetByJID(recipient.JID)
		puppet.SyncContactIfNecessary(source)
		err := puppet.DefaultIntent().EnsureJoined(portal.MXID)
		if err != nil {
			portal.log.Warnfln("Failed to make puppet of %s join %s: %v", recipient.JID, portal.MXID, err)
		}
	}
	portal.kickExtraUsers(participantMap)
}

func (portal *Portal) SyncParticipants(source *User, metadata *whatsapp.GroupInfo) {
	changed := false
	levels, err := portal.MainIntent().PowerLevels(portal.MXID)
	if err != nil {
		levels = portal.GetBasePowerLevels()
		changed = true
	}
	participantMap := make(map[whatsapp.JID]bool)
	for _, participant := range metadata.Participants {
		participantMap[participant.JID] = true
		user := portal.bridge.GetUserByJID(participant.JID)
		portal.userMXIDAction(user, portal.ensureMXIDInvited)

		puppet := portal.bridge.GetPuppetByJID(participant.JID)
		puppet.SyncContactIfNecessary(source)
		err = puppet.IntentFor(portal).EnsureJoined(portal.MXID)
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
	portal.kickExtraUsers(participantMap)
}

func (portal *Portal) UpdateAvatar(user *User, avatar *whatsapp.ProfilePicInfo, updateInfo bool) bool {
	if avatar == nil || (avatar.Status == 0 && avatar.Tag != "remove" && len(avatar.URL) == 0) {
		var err error
		avatar, err = user.Conn.GetProfilePicThumb(portal.Key.JID)
		if err != nil {
			portal.log.Errorln(err)
			return false
		}
	}

	if avatar.Status == 404 {
		avatar.Tag = "remove"
		avatar.Status = 0
	}
	if avatar.Status != 0 || portal.Avatar == avatar.Tag {
		return false
	}

	if avatar.Tag == "remove" {
		portal.AvatarURL = id.ContentURI{}
	} else {
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
	}

	if len(portal.MXID) > 0 {
		_, err := portal.MainIntent().SetRoomAvatar(portal.MXID, portal.AvatarURL)
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

func (portal *Portal) UpdateName(name string, setBy whatsapp.JID, intent *appservice.IntentAPI, updateInfo bool) bool {
	if name == "" && portal.IsBroadcastList() {
		name = UnnamedBroadcastName
	}
	if portal.Name != name {
		portal.log.Debugfln("Updating name %s -> %s", portal.Name, name)
		portal.Name = name
		if intent == nil {
			intent = portal.MainIntent()
			if len(setBy) > 0 {
				intent = portal.bridge.GetPuppetByJID(setBy).IntentFor(portal)
			}
		}
		_, err := intent.SetRoomName(portal.MXID, name)
		if err == nil {
			if updateInfo {
				portal.UpdateBridgeInfo()
			}
			return true
		} else {
			portal.Name = ""
			portal.log.Warnln("Failed to set room name:", err)
		}
	}
	return false
}

func (portal *Portal) UpdateTopic(topic string, setBy whatsapp.JID, intent *appservice.IntentAPI, updateInfo bool) bool {
	if portal.Topic != topic {
		portal.log.Debugfln("Updating topic %s -> %s", portal.Topic, topic)
		portal.Topic = topic
		if intent == nil {
			intent = portal.MainIntent()
			if len(setBy) > 0 {
				intent = portal.bridge.GetPuppetByJID(setBy).IntentFor(portal)
			}
		}
		_, err := intent.SetRoomTopic(portal.MXID, topic)
		if err == nil {
			if updateInfo {
				portal.UpdateBridgeInfo()
			}
			return true
		} else {
			portal.Topic = ""
			portal.log.Warnln("Failed to set room topic:", err)
		}
	}
	return false
}

func (portal *Portal) UpdateMetadata(user *User) bool {
	if portal.IsPrivateChat() {
		return false
	} else if portal.IsStatusBroadcastList() {
		update := false
		update = portal.UpdateName(StatusBroadcastName, "", nil, false) || update
		update = portal.UpdateTopic(StatusBroadcastTopic, "", nil, false) || update
		return update
	} else if portal.IsBroadcastList() {
		update := false
		broadcastMetadata, err := user.Conn.GetBroadcastMetadata(portal.Key.JID)
		if err == nil && broadcastMetadata.Status == 200 {
			portal.SyncBroadcastRecipients(user, broadcastMetadata)
			update = portal.UpdateName(broadcastMetadata.Name, "", nil, false) || update
		} else {
			user.Conn.Store.ContactsLock.RLock()
			contact, _ := user.Conn.Store.Contacts[portal.Key.JID]
			user.Conn.Store.ContactsLock.RUnlock()
			update = portal.UpdateName(contact.Name, "", nil, false) || update
		}
		update = portal.UpdateTopic(BroadcastTopic, "", nil, false) || update
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

	portal.SyncParticipants(user, metadata)
	update := false
	update = portal.UpdateName(metadata.Name, metadata.NameSetBy, nil, false) || update
	update = portal.UpdateTopic(metadata.Topic, metadata.TopicSetBy, nil, false) || update

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

func (portal *Portal) Sync(user *User, contact whatsapp.Contact) bool {
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
			return false
		}
	} else {
		portal.ensureUserInvited(user)
	}

	update := false
	update = portal.UpdateMetadata(user) || update
	if !portal.IsPrivateChat() && !portal.IsBroadcastList() && portal.Avatar == "" {
		update = portal.UpdateAvatar(user, nil, false) || update
	}
	if update {
		portal.Update()
		portal.UpdateBridgeInfo()
	}
	return true
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

func (portal *Portal) ChangeAdminStatus(jids []string, setAdmin bool) id.EventID {
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
		resp, err := portal.MainIntent().SetPowerLevels(portal.MXID, levels)
		if err != nil {
			portal.log.Errorln("Failed to change power levels:", err)
		} else {
			return resp.EventID
		}
	}
	return ""
}

func (portal *Portal) RestrictMessageSending(restrict bool) id.EventID {
	levels, err := portal.MainIntent().PowerLevels(portal.MXID)
	if err != nil {
		levels = portal.GetBasePowerLevels()
	}

	newLevel := 0
	if restrict {
		newLevel = 50
	}

	if levels.EventsDefault == newLevel {
		return ""
	}

	levels.EventsDefault = newLevel
	resp, err := portal.MainIntent().SetPowerLevels(portal.MXID, levels)
	if err != nil {
		portal.log.Errorln("Failed to change power levels:", err)
		return ""
	} else {
		return resp.EventID
	}
}

func (portal *Portal) RestrictMetadataChanges(restrict bool) id.EventID {
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
		resp, err := portal.MainIntent().SetPowerLevels(portal.MXID, levels)
		if err != nil {
			portal.log.Errorln("Failed to change power levels:", err)
		} else {
			return resp.EventID
		}
	}
	return ""
}

func (portal *Portal) BackfillHistory(user *User, lastMessageTime int64) error {
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
		if err == whatsapp.ErrServerRespondedWith404 {
			portal.log.Warnln("Got 404 response trying to fetch messages to backfill. Fetching latest messages as fallback.")
			resp, err = user.Conn.LoadMessagesBefore(portal.Key.JID, "", true, 50)
		}
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
	chunkNum := 0
	for n > 0 {
		chunkNum += 1
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
		portal.handleMessage(PortalMessage{portal.Key.JID, user, data, message.GetMessageTimestamp()}, true)
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

	var metadata *whatsapp.GroupInfo
	var broadcastMetadata *whatsapp.BroadcastListInfo
	if portal.IsPrivateChat() {
		puppet := portal.bridge.GetPuppetByJID(portal.Key.JID)
		puppet.SyncContactIfNecessary(user)
		if portal.bridge.Config.Bridge.PrivateChatPortalMeta {
			portal.Name = puppet.Displayname
			portal.AvatarURL = puppet.AvatarURL
			portal.Avatar = puppet.Avatar
		} else {
			portal.Name = ""
		}
		portal.Topic = PrivateChatTopic
	} else if portal.IsStatusBroadcastList() {
		if !portal.bridge.Config.Bridge.EnableStatusBroadcast {
			portal.log.Debugln("Status bridging is disabled in config, not creating room after all")
			return ErrStatusBroadcastDisabled
		}
		portal.Name = StatusBroadcastName
		portal.Topic = StatusBroadcastTopic
	} else if portal.IsBroadcastList() {
		var err error
		broadcastMetadata, err = user.Conn.GetBroadcastMetadata(portal.Key.JID)
		if err == nil && broadcastMetadata.Status == 200 {
			portal.Name = broadcastMetadata.Name
		} else {
			user.Conn.Store.ContactsLock.RLock()
			contact, _ := user.Conn.Store.Contacts[portal.Key.JID]
			user.Conn.Store.ContactsLock.RUnlock()
			portal.Name = contact.Name
		}
		if len(portal.Name) == 0 {
			portal.Name = UnnamedBroadcastName
		}
		portal.Topic = BroadcastTopic
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
		portal.SyncParticipants(user, metadata)
		if metadata.Announce {
			portal.RestrictMessageSending(metadata.Announce)
		}
	} else if !user.IsRelaybot {
		customPuppet := portal.bridge.GetPuppetByCustomMXID(user.MXID)
		if customPuppet != nil && customPuppet.CustomIntent() != nil {
			_ = customPuppet.CustomIntent().EnsureJoined(portal.MXID)
		}
	}
	if broadcastMetadata != nil {
		portal.SyncBroadcastRecipients(user, broadcastMetadata)
	}
	inCommunity := user.addPortalToCommunity(portal)
	if portal.IsPrivateChat() && !user.IsRelaybot {
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

	user.CreateUserPortal(database.PortalKeyWithMeta{PortalKey: portal.Key, InCommunity: inCommunity})

	err = portal.FillInitialHistory(user)
	if err != nil {
		portal.log.Errorln("Failed to fill history:", err)
	}
	return nil
}

func (portal *Portal) IsPrivateChat() bool {
	if portal.isPrivate == nil {
		val := strings.HasSuffix(portal.Key.JID, whatsapp.NewUserSuffix)
		portal.isPrivate = &val
	}
	return *portal.isPrivate
}

func (portal *Portal) IsBroadcastList() bool {
	if portal.isBroadcast == nil {
		val := strings.HasSuffix(portal.Key.JID, whatsapp.BroadcastSuffix)
		portal.isBroadcast = &val
	}
	return *portal.isBroadcast
}

func (portal *Portal) IsStatusBroadcastList() bool {
	return portal.Key.JID == "status@broadcast"
}

func (portal *Portal) HasRelaybot() bool {
	if portal.bridge.Relaybot == nil {
		return false
	} else if portal.hasRelaybot == nil {
		val := portal.bridge.Relaybot.IsInPortal(portal.Key)
		if !val {
			val = portal.bridge.Relaybot.CreateUserPortal(database.PortalKeyWithMeta{PortalKey: portal.Key, InCommunity: false})
		}
		portal.hasRelaybot = &val
	}
	return *portal.hasRelaybot
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
	if message != nil && !message.IsFakeMXID() {
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

func (portal *Portal) HandleMessageRevoke(user *User, message whatsapp.MessageRevocation) bool {
	msg := portal.bridge.DB.Message.GetByJID(portal.Key, message.Id)
	if msg == nil || msg.IsFakeMXID() {
		return false
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
	} else {
		msg.Delete()
	}
	return true
}

func (portal *Portal) HandleFakeMessage(_ *User, message FakeMessage) bool {
	if portal.isRecentlyHandled(message.ID) {
		return false
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
		return true
	}

	portal.recentlyHandledLock.Lock()
	index := portal.recentlyHandledIndex
	portal.recentlyHandledIndex = (portal.recentlyHandledIndex + 1) % recentlyHandledLength
	portal.recentlyHandledLock.Unlock()
	portal.recentlyHandled[index] = message.ID
	return true
}

func (portal *Portal) sendMainIntentMessage(content interface{}) (*mautrix.RespSendEvent, error) {
	return portal.sendMessage(portal.MainIntent(), event.EventMessage, content, 0)
}

func (portal *Portal) sendMessage(intent *appservice.IntentAPI, eventType event.Type, content interface{}, timestamp int64) (*mautrix.RespSendEvent, error) {
	wrappedContent := event.Content{Parsed: content}
	if timestamp != 0 && intent.IsCustomPuppet {
		wrappedContent.Raw = map[string]interface{}{
			"net.maunium.whatsapp.puppet": intent.IsCustomPuppet,
		}
	}
	if portal.Encrypted && portal.bridge.Crypto != nil {
		// TODO maybe the locking should be inside mautrix-go?
		portal.encryptLock.Lock()
		encrypted, err := portal.bridge.Crypto.Encrypt(portal.MXID, eventType, wrappedContent)
		portal.encryptLock.Unlock()
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt event: %w", err)
		}
		eventType = event.EventEncrypted
		wrappedContent.Parsed = encrypted
	}
	_, _ = intent.UserTyping(portal.MXID, false, 0)
	if timestamp == 0 {
		return intent.SendMessageEvent(portal.MXID, eventType, &wrappedContent)
	} else {
		return intent.SendMassagedMessageEvent(portal.MXID, eventType, &wrappedContent, timestamp)
	}
}

func (portal *Portal) HandleTextMessage(source *User, message whatsapp.TextMessage) bool {
	intent := portal.startHandling(source, message.Info, "text")
	if intent == nil {
		return false
	}

	content := &event.MessageEventContent{
		Body:    message.Text,
		MsgType: event.MsgText,
	}

	portal.bridge.Formatter.ParseWhatsApp(content, message.ContextInfo.MentionedJID)
	portal.SetReply(content, message.ContextInfo)

	resp, err := portal.sendMessage(intent, event.EventMessage, content, int64(message.Info.Timestamp*1000))
	if err != nil {
		portal.log.Errorfln("Failed to handle message %s: %v", message.Info.Id, err)
	} else {
		portal.finishHandling(source, message.Info.Source, resp.EventID)
	}
	return true
}

func (portal *Portal) HandleStubMessage(source *User, message whatsapp.StubMessage, isBackfill bool) bool {
	if portal.bridge.Config.Bridge.ChatMetaSync && (!portal.IsBroadcastList() || isBackfill) {
		// Chat meta sync is enabled, so we use chat update commands and full-syncs instead of message history
		// However, broadcast lists don't have update commands, so we handle these if it's not a backfill
		return false
	}
	intent := portal.startHandling(source, message.Info, fmt.Sprintf("stub %s", message.Type.String()))
	if intent == nil {
		return false
	}
	var senderJID string
	if message.Info.FromMe {
		senderJID = source.JID
	} else {
		senderJID = message.Info.SenderJid
	}
	var eventID id.EventID
	// TODO find more real event IDs
	// TODO timestamp massaging
	switch message.Type {
	case waProto.WebMessageInfo_GROUP_CHANGE_SUBJECT:
		portal.UpdateName(message.FirstParam, "", intent, true)
	case waProto.WebMessageInfo_GROUP_CHANGE_ICON:
		portal.UpdateAvatar(source, nil, true)
	case waProto.WebMessageInfo_GROUP_CHANGE_DESCRIPTION:
		if isBackfill {
			// TODO fetch topic from server
		}
		//portal.UpdateTopic(message.FirstParam, "", intent, true)
	case waProto.WebMessageInfo_GROUP_CHANGE_ANNOUNCE:
		eventID = portal.RestrictMessageSending(message.FirstParam == "on")
	case waProto.WebMessageInfo_GROUP_CHANGE_RESTRICT:
		eventID = portal.RestrictMetadataChanges(message.FirstParam == "on")
	case waProto.WebMessageInfo_GROUP_PARTICIPANT_ADD, waProto.WebMessageInfo_GROUP_PARTICIPANT_INVITE, waProto.WebMessageInfo_BROADCAST_ADD:
		eventID = portal.HandleWhatsAppInvite(source, senderJID, intent, message.Params)
	case waProto.WebMessageInfo_GROUP_PARTICIPANT_REMOVE, waProto.WebMessageInfo_GROUP_PARTICIPANT_LEAVE, waProto.WebMessageInfo_BROADCAST_REMOVE:
		portal.HandleWhatsAppKick(source, senderJID, message.Params)
	case waProto.WebMessageInfo_GROUP_PARTICIPANT_PROMOTE:
		eventID = portal.ChangeAdminStatus(message.Params, true)
	case waProto.WebMessageInfo_GROUP_PARTICIPANT_DEMOTE:
		eventID = portal.ChangeAdminStatus(message.Params, false)
	default:
		return false
	}
	if len(eventID) == 0 {
		eventID = id.EventID(fmt.Sprintf("net.maunium.whatsapp.fake::%s", message.Info.Id))
	}
	portal.markHandled(source, message.Info.Source, eventID, true)
	return true
}

func (portal *Portal) HandleLocationMessage(source *User, message whatsapp.LocationMessage) bool {
	intent := portal.startHandling(source, message.Info, "location")
	if intent == nil {
		return false
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

	resp, err := portal.sendMessage(intent, event.EventMessage, content, int64(message.Info.Timestamp*1000))
	if err != nil {
		portal.log.Errorfln("Failed to handle message %s: %v", message.Info.Id, err)
	} else {
		portal.finishHandling(source, message.Info.Source, resp.EventID)
	}
	return true
}

func (portal *Portal) HandleContactMessage(source *User, message whatsapp.ContactMessage) bool {
	intent := portal.startHandling(source, message.Info, "contact")
	if intent == nil {
		return false
	}

	fileName := fmt.Sprintf("%s.vcf", message.DisplayName)
	data := []byte(message.Vcard)
	mimeType := "text/vcard"
	data, uploadMimeType, file := portal.encryptFile(data, mimeType)

	uploadResp, err := intent.UploadBytesWithName(data, uploadMimeType, fileName)
	if err != nil {
		portal.log.Errorfln("Failed to upload vcard of %s: %v", message.DisplayName, err)
		return true
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

	resp, err := portal.sendMessage(intent, event.EventMessage, content, int64(message.Info.Timestamp*1000))
	if err != nil {
		portal.log.Errorfln("Failed to handle message %s: %v", message.Info.Id, err)
	} else {
		portal.finishHandling(source, message.Info.Source, resp.EventID)
	}
	return true
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

func (portal *Portal) HandleWhatsAppKick(source *User, senderJID string, jids []string) {
	sender := portal.bridge.GetPuppetByJID(senderJID)
	senderIntent := sender.IntentFor(portal)
	for _, jid := range jids {
		if source != nil && source.JID == jid {
			portal.log.Debugln("Ignoring self-kick by", source.MXID)
			continue
		}
		puppet := portal.bridge.GetPuppetByJID(jid)
		portal.removeUser(puppet.JID == sender.JID, senderIntent, puppet.MXID, puppet.DefaultIntent())

		if !portal.IsBroadcastList() {
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
}

func (portal *Portal) HandleWhatsAppInvite(source *User, senderJID string, intent *appservice.IntentAPI, jids []string) (evtID id.EventID) {
	if intent == nil {
		intent = portal.MainIntent()
		if senderJID != "unknown" {
			sender := portal.bridge.GetPuppetByJID(senderJID)
			intent = sender.IntentFor(portal)
		}
	}
	for _, jid := range jids {
		puppet := portal.bridge.GetPuppetByJID(jid)
		puppet.SyncContactIfNecessary(source)
		content := event.Content{
			Parsed: event.MemberEventContent{
				Membership:  "invite",
				Displayname: puppet.Displayname,
				AvatarURL:   puppet.AvatarURL.CUString(),
			},
			Raw: map[string]interface{}{
				"net.maunium.whatsapp.puppet": true,
			},
		}
		resp, err := intent.SendStateEvent(portal.MXID, event.StateMember, puppet.MXID.String(), &content)
		if err != nil {
			portal.log.Warnfln("Failed to invite %s as %s: %v", puppet.MXID, intent.UserID, err)
			_ = portal.MainIntent().EnsureInvited(portal.MXID, puppet.MXID)
		} else {
			evtID = resp.EventID
		}
		err = puppet.DefaultIntent().EnsureJoined(portal.MXID)
		if err != nil {
			portal.log.Errorfln("Failed to ensure %s is joined: %v", puppet.MXID, err)
		}
	}
	return
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

func (portal *Portal) HandleMediaMessage(source *User, msg mediaMessage) bool {
	intent := portal.startHandling(source, msg.info, fmt.Sprintf("media %s", msg.mimeType))
	if intent == nil {
		return false
	}

	data, err := msg.download()
	if err == whatsapp.ErrMediaDownloadFailedWith404 || err == whatsapp.ErrMediaDownloadFailedWith410 {
		portal.log.Warnfln("Failed to download media for %s: %v. Calling LoadMediaInfo and retrying download...", msg.info.Id, err)
		_, err = source.Conn.LoadMediaInfo(msg.info.RemoteJid, msg.info.Id, msg.info.FromMe)
		if err != nil {
			portal.sendMediaBridgeFailure(source, intent, msg.info, fmt.Errorf("failed to load media info: %w", err))
			return true
		}
		data, err = msg.download()
	}
	if err == whatsapp.ErrNoURLPresent {
		portal.log.Debugfln("No URL present error for media message %s, ignoring...", msg.info.Id)
		return true
	} else if err != nil {
		portal.sendMediaBridgeFailure(source, intent, msg.info, err)
		return true
	}

	var width, height int
	if strings.HasPrefix(msg.mimeType, "image/") {
		cfg, _, _ := image.DecodeConfig(bytes.NewReader(data))
		width, height = cfg.Width, cfg.Height
	}

	data, uploadMimeType, file := portal.encryptFile(data, msg.mimeType)

	uploaded, err := intent.UploadBytes(data, uploadMimeType)
	if err != nil {
		if errors.Is(err, mautrix.MTooLarge) {
			portal.sendMediaBridgeFailure(source, intent, msg.info, errors.New("homeserver rejected too large file"))
		} else if httpErr, ok := err.(mautrix.HTTPError); ok && httpErr.IsStatus(413) {
			portal.sendMediaBridgeFailure(source, intent, msg.info, errors.New("proxy rejected too large file"))
		} else {
			portal.sendMediaBridgeFailure(source, intent, msg.info, fmt.Errorf("failed to upload media: %w", err))
		}
		return true
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

	ts := int64(msg.info.Timestamp * 1000)
	eventType := event.EventMessage
	if msg.sendAsSticker {
		eventType = event.EventSticker
	}
	resp, err := portal.sendMessage(intent, eventType, content, ts)
	if err != nil {
		portal.log.Errorfln("Failed to handle message %s: %v", msg.info.Id, err)
		return true
	}

	if len(msg.caption) > 0 {
		captionContent := &event.MessageEventContent{
			Body:    msg.caption,
			MsgType: event.MsgNotice,
		}

		portal.bridge.Formatter.ParseWhatsApp(captionContent, msg.context.MentionedJID)

		resp, err = portal.sendMessage(intent, event.EventMessage, captionContent, ts)
		if err != nil {
			portal.log.Warnfln("Failed to handle caption of message %s: %v", msg.info.Id, err)
		}
	}

	portal.finishHandling(source, msg.info.Source, resp.EventID)
	return true
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
	vcLog := portal.log.Sub("VideoConverter").Writer(log.LevelWarn)
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

func (portal *Portal) preprocessMatrixMedia(sender *User, relaybotFormatted bool, content *event.MessageEventContent, eventID id.EventID, mediaType whatsapp.MediaType) *MediaUpload {
	var caption string
	var mentionedJIDs []whatsapp.JID
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
		return nil
	}
	data, err := portal.MainIntent().DownloadBytes(mxc)
	if err != nil {
		portal.log.Errorfln("Failed to download media in %s: %v", eventID, err)
		return nil
	}
	if file != nil {
		data, err = file.Decrypt(data)
		if err != nil {
			portal.log.Errorfln("Failed to decrypt media in %s: %v", eventID, err)
			return nil
		}
	}
	if mediaType == whatsapp.MediaVideo && content.GetInfo().MimeType == "image/gif" {
		data, err = portal.convertGifToVideo(data)
		if err != nil {
			portal.log.Errorfln("Failed to convert gif to mp4 in %s: %v", eventID, err)
			return nil
		}
		content.Info.MimeType = "video/mp4"
	}

	url, mediaKey, fileEncSHA256, fileSHA256, fileLength, err := sender.Conn.Upload(bytes.NewReader(data), mediaType)
	if err != nil {
		portal.log.Errorfln("Failed to upload media in %s: %v", eventID, err)
		return nil
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
	}
}

type MediaUpload struct {
	Caption       string
	MentionedJIDs []whatsapp.JID
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
		inRoom := ""
		if portal.IsPrivateChat() {
			inRoom = " in your management room"
		}
		if sender.IsLoginInProgress() {
			portal.log.Debugln("Waiting for connection before handling event", eventID, "from", sender.MXID)
			sender.Conn.WaitForLogin()
			if sender.IsConnected() {
				return false
			}
		}
		reconnect := fmt.Sprintf("Use `%s reconnect`%s to reconnect.", portal.bridge.Config.Bridge.CommandPrefix, inRoom)
		portal.log.Debugln("Ignoring event", eventID, "from", sender.MXID, "as user is not connected")
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

func addCodecToMime(mimeType, codec string) string {
	mediaType, params, err := mime.ParseMediaType(mimeType)
	if err != nil {
		return mimeType
	}
	if _, ok := params["codecs"]; !ok {
		params["codecs"] = codec
	}
	return mime.FormatMediaType(mediaType, params)
}

func (portal *Portal) convertMatrixMessage(sender *User, evt *event.Event) (*waProto.WebMessageInfo, *User) {
	content, ok := evt.Content.Parsed.(*event.MessageEventContent)
	if !ok {
		portal.log.Debugfln("Failed to handle event %s: unexpected parsed content type %T", evt.ID, evt.Content.Parsed)
		return nil, sender
	}

	ts := uint64(evt.Timestamp / 1000)
	status := waProto.WebMessageInfo_PENDING
	trueVal := true
	info := &waProto.WebMessageInfo{
		Key: &waProto.MessageKey{
			FromMe:    &trueVal,
			Id:        makeMessageID(),
			RemoteJid: &portal.Key.JID,
		},
		MessageTimestamp:    &ts,
		MessageC2STimestamp: &ts,
		Message:             &waProto.Message{},
		Status:              &status,
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
				return nil, sender
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
		if content.MsgType == event.MsgNotice && !portal.bridge.Config.Bridge.BridgeNotices {
			return nil, sender
		}
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
		media := portal.preprocessMatrixMedia(sender, relaybotFormatted, content, evt.ID, whatsapp.MediaImage)
		if media == nil {
			return nil, sender
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
		media := portal.preprocessMatrixMedia(sender, relaybotFormatted, content, evt.ID, whatsapp.MediaVideo)
		if media == nil {
			return nil, sender
		}
		duration := uint32(content.GetInfo().Duration / 1000)
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
		media := portal.preprocessMatrixMedia(sender, relaybotFormatted, content, evt.ID, whatsapp.MediaAudio)
		if media == nil {
			return nil, sender
		}
		duration := uint32(content.GetInfo().Duration / 1000)
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
		_, isMSC3245Voice := evt.Content.Raw["org.matrix.msc3245.voice"]
		_, isMSC2516Voice := evt.Content.Raw["org.matrix.msc2516.voice"]
		if isMSC3245Voice || isMSC2516Voice {
			info.Message.AudioMessage.Ptt = &trueVal
			// hacky hack to add the codecs param that whatsapp seems to require
			mimeWithCodec := addCodecToMime(content.GetInfo().MimeType, "opus")
			info.Message.AudioMessage.Mimetype = &mimeWithCodec
		}
	case event.MsgFile:
		media := portal.preprocessMatrixMedia(sender, relaybotFormatted, content, evt.ID, whatsapp.MediaDocument)
		if media == nil {
			return nil, sender
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
		portal.log.Debugfln("Unhandled Matrix event %s: unknown msgtype %s", evt.ID, content.MsgType)
		return nil, sender
	}
	return info, sender
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

func (portal *Portal) sendErrorMessage(message string, confirmed bool) id.EventID {
	certainty := "may not have been"
	if confirmed {
		certainty = "was not"
	}
	resp, err := portal.sendMainIntentMessage(event.MessageEventContent{
		MsgType: event.MsgNotice,
		Body:    fmt.Sprintf("\u26a0 Your message %s bridged: %v", certainty, message),
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

func (portal *Portal) HandleMatrixMessage(sender *User, evt *event.Event) {
	if !portal.HasRelaybot() && (
		(portal.IsPrivateChat() && sender.JID != portal.Key.Receiver) ||
			portal.sendMatrixConnectionError(sender, evt.ID)) {
		return
	}
	portal.log.Debugfln("Received event %s", evt.ID)
	info, sender := portal.convertMatrixMessage(sender, evt)
	if info == nil {
		return
	}
	dbMsg := portal.markHandled(sender, info, evt.ID, false)
	portal.sendRaw(sender, evt, info, dbMsg)
}

func (portal *Portal) sendRaw(sender *User, evt *event.Event, info *waProto.WebMessageInfo, dbMsg *database.Message) {
	portal.log.Debugln("Sending event", evt.ID, "to WhatsApp", info.Key.GetId())
	errChan := make(chan error, 1)
	go sender.Conn.SendRaw(info, errChan)

	var err error
	var errorEventID id.EventID
	select {
	case err = <-errChan:
	case <-time.After(time.Duration(portal.bridge.Config.Bridge.ConnectionTimeout) * time.Second):
		if portal.bridge.Config.Bridge.FetchMessageOnTimeout && portal.wasMessageSent(sender, info.Key.GetId()) {
			portal.log.Debugln("Matrix event %s was bridged, but response didn't arrive within timeout")
			portal.sendDeliveryReceipt(evt.ID)
		} else {
			portal.log.Warnfln("Response when bridging Matrix event %s is taking long to arrive", evt.ID)
			errorEventID = portal.sendErrorMessage("message sending timed out", false)
		}
		err = <-errChan
	}
	if err != nil {
		var statusErr whatsapp.StatusResponse
		errors.As(err, &statusErr)
		var confirmed bool
		var errMsg string
		switch statusErr.Status {
		case 400:
			portal.log.Errorfln("400 response handling Matrix event %s: %+v", evt.ID, statusErr.Extra)
			errMsg = "WhatsApp rejected the message (status code 400)."
			if info.Message.ImageMessage != nil || info.Message.VideoMessage != nil || info.Message.AudioMessage != nil || info.Message.DocumentMessage != nil {
				errMsg += " The attachment type you sent may be unsupported."
			}
			confirmed = true
		case 599:
			errMsg = "WhatsApp rate-limited the message (status code 599)."
		default:
			portal.log.Errorfln("Error handling Matrix event %s: %v", evt.ID, err)
			errMsg = err.Error()
		}
		portal.sendErrorMessage(errMsg, confirmed)
	} else {
		portal.log.Debugfln("Handled Matrix event %s", evt.ID)
		portal.sendDeliveryReceipt(evt.ID)
		dbMsg.MarkSent()
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
	} else if portal.bridge.Config.Bridge.BridgeMatrixLeave {
		// TODO should we somehow deduplicate this call if this leave was sent by the bridge?
		resp, err := sender.Conn.LeaveGroup(portal.Key.JID)
		if err != nil {
			portal.log.Errorfln("Failed to leave group as %s: %v", sender.MXID, err)
			return
		}
		portal.log.Infoln("Leave response:", <-resp)
	}
	portal.CleanupIfEmpty()
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
		portal.log.Infofln("Add %s response: %s", puppet.JID, <-resp)
	}
}

func (portal *Portal) HandleMatrixMeta(sender *User, evt *event.Event) {
	var resp <-chan string
	var err error
	switch content := evt.Content.Parsed.(type) {
	case *event.RoomNameEventContent:
		if content.Name == portal.Name {
			return
		}
		portal.Name = content.Name
		resp, err = sender.Conn.UpdateGroupSubject(content.Name, portal.Key.JID)
	case *event.TopicEventContent:
		if content.Topic == portal.Topic {
			return
		}
		portal.Topic = content.Topic
		resp, err = sender.Conn.UpdateGroupDescription(sender.JID, portal.Key.JID, content.Topic)
	case *event.RoomAvatarEventContent:
		return
	}
	if err != nil {
		portal.log.Errorln("Failed to update metadata:", err)
	} else {
		out := <-resp
		portal.log.Debugln("Successfully updated metadata:", out)
	}
}
