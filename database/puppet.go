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

	log "maunium.net/go/maulogger/v2"

	"github.com/Rhymen/go-whatsapp"

	"maunium.net/go/mautrix/id"
)

type PuppetQuery struct {
	db  *Database
	log log.Logger
}

func (pq *PuppetQuery) New() *Puppet {
	return &Puppet{
		db:  pq.db,
		log: pq.log,

		EnablePresence: true,
		EnableReceipts: true,
	}
}

func (pq *PuppetQuery) GetAll() (puppets []*Puppet) {
	rows, err := pq.db.Query("SELECT jid, avatar, avatar_url, displayname, name_quality, custom_mxid, access_token, next_batch, enable_presence, enable_receipts, first_activity_ts, last_activity_ts FROM puppet")
	if err != nil || rows == nil {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		puppets = append(puppets, pq.New().Scan(rows))
	}
	return
}

func (pq *PuppetQuery) Get(jid whatsapp.JID) *Puppet {
	row := pq.db.QueryRow("SELECT jid, avatar, avatar_url, displayname, name_quality, custom_mxid, access_token, next_batch, enable_presence, enable_receipts, first_activity_ts, last_activity_ts FROM puppet WHERE jid=$1", jid)
	if row == nil {
		return nil
	}
	return pq.New().Scan(row)
}

func (pq *PuppetQuery) GetByCustomMXID(mxid id.UserID) *Puppet {
	row := pq.db.QueryRow("SELECT jid, avatar, avatar_url, displayname, name_quality, custom_mxid, access_token, next_batch, enable_presence, enable_receipts, first_activity_ts, last_activity_ts FROM puppet WHERE custom_mxid=$1", mxid)
	if row == nil {
		return nil
	}
	return pq.New().Scan(row)
}

func (pq *PuppetQuery) GetAllWithCustomMXID() (puppets []*Puppet) {
	rows, err := pq.db.Query("SELECT jid, avatar, avatar_url, displayname, name_quality, custom_mxid, access_token, next_batch, enable_presence, enable_receipts, first_activity_ts, last_activity_ts FROM puppet WHERE custom_mxid<>''")
	if err != nil || rows == nil {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		puppets = append(puppets, pq.New().Scan(rows))
	}
	return
}

type Puppet struct {
	db  *Database
	log log.Logger

	JID         whatsapp.JID
	Avatar      string
	AvatarURL   id.ContentURI
	Displayname string
	NameQuality int8

	CustomMXID      id.UserID
	AccessToken     string
	NextBatch       string
	EnablePresence  bool
	EnableReceipts  bool
	FirstActivityTs int64
	LastActivityTs  int64
}

func (puppet *Puppet) Scan(row Scannable) *Puppet {
	var displayname, avatar, avatarURL, customMXID, accessToken, nextBatch sql.NullString
	var quality, firstActivityTs, lastActivityTs sql.NullInt64
	var enablePresence, enableReceipts sql.NullBool
	err := row.Scan(&puppet.JID, &avatar, &avatarURL, &displayname, &quality, &customMXID, &accessToken, &nextBatch, &enablePresence, &enableReceipts, &firstActivityTs, &lastActivityTs)
	if err != nil {
		if err != sql.ErrNoRows {
			puppet.log.Errorln("Database scan failed:", err)
		}
		return nil
	}
	puppet.Displayname = displayname.String
	puppet.Avatar = avatar.String
	puppet.AvatarURL, _ = id.ParseContentURI(avatarURL.String)
	puppet.NameQuality = int8(quality.Int64)
	puppet.CustomMXID = id.UserID(customMXID.String)
	puppet.AccessToken = accessToken.String
	puppet.NextBatch = nextBatch.String
	puppet.EnablePresence = enablePresence.Bool
	puppet.EnableReceipts = enableReceipts.Bool
	puppet.FirstActivityTs = firstActivityTs.Int64
	puppet.LastActivityTs = lastActivityTs.Int64
	return puppet
}

func (puppet *Puppet) Insert() {
	_, err := puppet.db.Exec("INSERT INTO puppet (jid, avatar, avatar_url, displayname, name_quality, custom_mxid, access_token, next_batch, enable_presence, enable_receipts) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		puppet.JID, puppet.Avatar, puppet.AvatarURL.String(), puppet.Displayname, puppet.NameQuality, puppet.CustomMXID, puppet.AccessToken, puppet.NextBatch, puppet.EnablePresence, puppet.EnableReceipts)
	if err != nil {
		puppet.log.Warnfln("Failed to insert %s: %v", puppet.JID, err)
	}
}

func (puppet *Puppet) Update() {
	_, err := puppet.db.Exec("UPDATE puppet SET displayname=$1, name_quality=$2, avatar=$3, avatar_url=$4, custom_mxid=$5, access_token=$6, next_batch=$7, enable_presence=$8, enable_receipts=$9 WHERE jid=$10",
		puppet.Displayname, puppet.NameQuality, puppet.Avatar, puppet.AvatarURL.String(), puppet.CustomMXID, puppet.AccessToken, puppet.NextBatch, puppet.EnablePresence, puppet.EnableReceipts, puppet.JID)
	if err != nil {
		puppet.log.Warnfln("Failed to update %s: %v", puppet.JID, err)
	}
}

func (puppet *Puppet) UpdateActivityTs(ts uint64) {
	var signedTs = int64(ts)
	if puppet.LastActivityTs > signedTs {
		return
	}
	puppet.log.Debugfln("Updating activity time for %s to %d", puppet.JID, signedTs)
	puppet.LastActivityTs = signedTs
	_, err := puppet.db.Exec("UPDATE puppet SET last_activity_ts=$1 WHERE jid=$2", puppet.LastActivityTs, puppet.JID)
	if err != nil {
		puppet.log.Warnfln("Failed to update last_activity_ts for %s: %v", puppet.JID, err)
	}

	if puppet.FirstActivityTs == 0 {
		puppet.FirstActivityTs = signedTs
		_, err = puppet.db.Exec("UPDATE puppet SET first_activity_ts=$1 WHERE jid=$2 AND first_activity_ts is NULL", puppet.FirstActivityTs, puppet.JID)
		if err != nil {
			puppet.log.Warnfln("Failed to update first_activity_ts %s: %v", puppet.JID, err)
		}
	}
}
