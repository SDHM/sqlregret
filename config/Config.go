package config

import (
	"io/ioutil"

	"github.com/siddontang/go-yaml/yaml"
)

type InstanceConfig struct {
	Destination       string `yaml:"destination"`
	SlaveId           string `yaml:"slaveId"`
	MasterAddress     string `yaml:"masterAddress"`
	MasterPort        string `yaml:"masterPort"`
	MasterJournalName string `yaml:"masterJournalName"`
	MasterPosition    string `yaml:"masterPosition"`
	DbUsername        string `yaml:"dbUsername"`
	DbPassword        string `yaml:"dbPassword"`
	DefaultDbName     string `yaml:"defaultDbName"`
}

type Config struct {
	InstancesConfig []InstanceConfig `yaml:"instances"`
}

func ParseConfigData(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal([]byte(data), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func ParseConfigFile(fileName string) (*Config, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	return ParseConfigData(data)
}
