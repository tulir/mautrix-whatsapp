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

package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Rhymen/go-whatsapp"

	log "maunium.net/go/maulogger/v2"

	"maunium.net/go/mautrix/id"
)

type UserQuery struct {
	db  *Database
	log log.Logger
}

func (uq *UserQuery) New() *User {
	return &User{
		db:  uq.db,
		log: uq.log,
	}
}

func (uq *UserQuery) GetAll() (users []*User) {
	rows, err := uq.db.Query(`SELECT mxid, jid, management_room, last_connection, client_id, client_token, server_token, enc_key, mac_key FROM "user"`)
	if err != nil || rows == nil {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		users = append(users, uq.New().Scan(rows))
	}
	return
}

func (uq *UserQuery) GetByMXID(userID id.UserID) *User {
	row := uq.db.QueryRow(`SELECT mxid, jid, management_room, last_connection, client_id, client_token, server_token, enc_key, mac_key FROM "user" WHERE mxid=$1`, userID)
	if row == nil {
		return nil
	}
	return uq.New().Scan(row)
}

func (uq *UserQuery) GetByJID(userID whatsapp.JID) *User {
	row := uq.db.QueryRow(`SELECT mxid, jid, management_room, last_connection, client_id, client_token, server_token, enc_key, mac_key FROM "user" WHERE jid=$1`, stripSuffix(userID))
	if row == nil {
		return nil
	}
	return uq.New().Scan(row)
}

type User struct {
	db  *Database
	log log.Logger

	MXID           id.UserID
	JID            whatsapp.JID
	ManagementRoom id.RoomID
	Session        *whatsapp.Session
	LastConnection int64
}

func (user *User) Scan(row Scannable) *User {
	var jid, clientID, clientToken, serverToken sql.NullString
	var encKey, macKey []byte
	err := row.Scan(&user.MXID, &jid, &user.ManagementRoom, &user.LastConnection, &clientID, &clientToken, &serverToken, &encKey, &macKey)
	if err != nil {
		if err != sql.ErrNoRows {
			user.log.Errorln("Database scan failed:", err)
		}
		return nil
	}
	if len(jid.String) > 0 && len(clientID.String) > 0 {
		user.JID = jid.String + whatsapp.NewUserSuffix
		user.Session = &whatsapp.Session{
			ClientID:    clientID.String,
			ClientToken: clientToken.String,
			ServerToken: serverToken.String,
			EncKey:      encKey,
			MacKey:      macKey,
			Wid:         jid.String + whatsapp.OldUserSuffix,
		}
	} else {
		user.Session = nil
	}
	return user
}

func stripSuffix(jid whatsapp.JID) string {
	if len(jid) == 0 {
		return jid
	}

	index := strings.IndexRune(jid, '@')
	if index < 0 {
		return jid
	}

	return jid[:index]
}

func (user *User) jidPtr() *string {
	if len(user.JID) > 0 {
		str := stripSuffix(user.JID)
		return &str
	}
	return nil
}

func (user *User) sessionUnptr() (sess whatsapp.Session) {
	if user.Session != nil {
		sess = *user.Session
	}
	return
}

func (user *User) Insert() {
	sess := user.sessionUnptr()
	_, err := user.db.Exec(`INSERT INTO "user" (mxid, jid, management_room, last_connection, client_id, client_token, server_token, enc_key, mac_key) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		user.MXID, user.jidPtr(),
		user.ManagementRoom, user.LastConnection,
		sess.ClientID, sess.ClientToken, sess.ServerToken, sess.EncKey, sess.MacKey)
	if err != nil {
		user.log.Warnfln("Failed to insert %s: %v", user.MXID, err)
	}
}

func (user *User) UpdateLastConnection() {
	user.LastConnection = time.Now().Unix()
	_, err := user.db.Exec(`UPDATE "user" SET last_connection=$1 WHERE mxid=$2`,
		user.LastConnection, user.MXID)
	if err != nil {
		user.log.Warnfln("Failed to update last connection ts: %v", err)
	}
}

func (user *User) Update() {
	sess := user.sessionUnptr()
	_, err := user.db.Exec(`UPDATE "user" SET jid=$1, management_room=$2, last_connection=$3, client_id=$4, client_token=$5, server_token=$6, enc_key=$7, mac_key=$8 WHERE mxid=$9`,
		user.jidPtr(), user.ManagementRoom, user.LastConnection,
		sess.ClientID, sess.ClientToken, sess.ServerToken, sess.EncKey, sess.MacKey,
		user.MXID)
	if err != nil {
		user.log.Warnfln("Failed to update %s: %v", user.MXID, err)
	}
}

type PortalKeyWithMeta struct {
	PortalKey
	InCommunity bool
}

func (user *User) SetPortalKeys(newKeys []PortalKeyWithMeta) error {
	tx, err := user.db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec("DELETE FROM user_portal WHERE user_jid=$1", user.jidPtr())
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	valueStrings := make([]string, len(newKeys))
	values := make([]interface{}, len(newKeys)*4)
	for i, key := range newKeys {
		pos := i * 4
		valueStrings[i] = fmt.Sprintf("($%d, $%d, $%d, $%d)", pos+1, pos+2, pos+3, pos+4)
		values[pos] = user.jidPtr()
		values[pos+1] = key.JID
		values[pos+2] = key.Receiver
		values[pos+3] = key.InCommunity
	}
	query := fmt.Sprintf("INSERT INTO user_portal (user_jid, portal_jid, portal_receiver, in_community) VALUES %s",
		strings.Join(valueStrings, ", "))
	_, err = tx.Exec(query, values...)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (user *User) IsInPortal(key PortalKey) bool {
	row := user.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM user_portal WHERE user_jid=$1 AND portal_jid=$2 AND portal_receiver=$3)`, user.jidPtr(), &key.JID, &key.Receiver)
	var exists bool
	_ = row.Scan(&exists)
	return exists
}

func (user *User) GetPortalKeys() []PortalKey {
	rows, err := user.db.Query(`SELECT portal_jid, portal_receiver FROM user_portal WHERE user_jid=$1`, user.jidPtr())
	if err != nil {
		user.log.Warnln("Failed to get user portal keys:", err)
		return nil
	}
	var keys []PortalKey
	for rows.Next() {
		var key PortalKey
		err = rows.Scan(&key.JID, &key.Receiver)
		if err != nil {
			user.log.Warnln("Failed to scan row:", err)
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

func (user *User) GetInCommunityMap() map[PortalKey]bool {
	rows, err := user.db.Query(`SELECT portal_jid, portal_receiver, in_community FROM user_portal WHERE user_jid=$1`, user.jidPtr())
	if err != nil {
		user.log.Warnln("Failed to get user portal keys:", err)
		return nil
	}
	keys := make(map[PortalKey]bool)
	for rows.Next() {
		var key PortalKey
		var inCommunity bool
		err = rows.Scan(&key.JID, &key.Receiver, &inCommunity)
		if err != nil {
			user.log.Warnln("Failed to scan row:", err)
			continue
		}
		keys[key] = inCommunity
	}
	return keys
}

func (user *User) CreateUserPortal(newKey PortalKeyWithMeta) bool {
	user.log.Debugfln("Creating new portal %s for %s", newKey.PortalKey.JID, newKey.PortalKey.Receiver)
	_, err := user.db.Exec(`INSERT INTO user_portal (user_jid, portal_jid, portal_receiver, in_community) VALUES ($1, $2, $3, $4)`,
		user.jidPtr(),
		newKey.PortalKey.JID, newKey.PortalKey.Receiver,
		newKey.InCommunity)
	if err != nil {
		user.log.Warnfln("Failed to insert %s: %v", user.MXID, err)
		return false
	}
	return true
}