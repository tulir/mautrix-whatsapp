// mautrix-whatsapp - A Matrix-WhatsApp puppeting bridge.
// Copyright (C) 2019 Tulir Asokan
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

package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
	"maunium.net/go/mautrix/id"

	"maunium.net/go/mautrix/appservice"
)

type Config struct {
	Homeserver struct {
		Address        string `yaml:"address"`
		Domain         string `yaml:"domain"`
		Asmux          bool   `yaml:"asmux"`
		StatusEndpoint string `yaml:"status_endpoint"`
	} `yaml:"homeserver"`

	AppService struct {
		Address  string `yaml:"address"`
		Hostname string `yaml:"hostname"`
		Port     uint16 `yaml:"port"`

		Database struct {
			Type string `yaml:"type"`
			URI  string `yaml:"uri"`

			MaxOpenConns int `yaml:"max_open_conns"`
			MaxIdleConns int `yaml:"max_idle_conns"`
		} `yaml:"database"`

		StateStore string `yaml:"state_store_path,omitempty"`

		Provisioning struct {
			Prefix       string `yaml:"prefix"`
			SharedSecret string `yaml:"shared_secret"`
		} `yaml:"provisioning"`

		ID  string `yaml:"id"`
		Bot struct {
			Username    string `yaml:"username"`
			Displayname string `yaml:"displayname"`
			Avatar      string `yaml:"avatar"`
		} `yaml:"bot"`

		Limits struct {
			MaxPuppetLimit      uint `yaml:"max_puppet_limit"`
			MinPuppetActiveDays uint `yaml:"min_puppet_activity_days"`
			BlockOnLimitReached bool `yaml:"block_on_limit_reached"`
		} `yaml:"limits"`

		ASToken string `yaml:"as_token"`
		HSToken string `yaml:"hs_token"`
	} `yaml:"appservice"`

	Metrics struct {
		Enabled bool   `yaml:"enabled"`
		Listen  string `yaml:"listen"`
	} `yaml:"metrics"`

	WhatsApp struct {
		OSName      string `yaml:"os_name"`
		BrowserName string `yaml:"browser_name"`
	} `yaml:"whatsapp"`

	Bridge BridgeConfig `yaml:"bridge"`

	Logging appservice.LogConfig `yaml:"logging"`
}

func (config *Config) CanDoublePuppet(userID id.UserID) bool {
	if len(config.Bridge.LoginSharedSecret) == 0 {
		// Automatic login not enabled
		return false
	} else if _, homeserver, _ := userID.Parse(); homeserver != config.Homeserver.Domain {
		// user is on another homeserver
		return false
	}
	return true
}

func (config *Config) setDefaults() {
	config.AppService.Database.MaxOpenConns = 20
	config.AppService.Database.MaxIdleConns = 2
	config.AppService.Limits.MaxPuppetLimit = 0
	config.AppService.Limits.MinPuppetActiveDays = 0
	config.AppService.Limits.BlockOnLimitReached = false
	config.WhatsApp.OSName = "Mautrix-WhatsApp bridge"
	config.WhatsApp.BrowserName = "mx-wa"
	config.Bridge.setDefaults()
}

func Load(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config = &Config{}
	config.setDefaults()
	err = yaml.Unmarshal(data, config)
	return config, err
}

func (config *Config) Save(path string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, data, 0600)
}

func (config *Config) MakeAppService() (*appservice.AppService, error) {
	as := appservice.Create()
	as.HomeserverDomain = config.Homeserver.Domain
	as.HomeserverURL = config.Homeserver.Address
	as.Host.Hostname = config.AppService.Hostname
	as.Host.Port = config.AppService.Port
	as.DefaultHTTPRetries = 4
	var err error
	as.Registration, err = config.GetRegistration()
	return as, err
}
