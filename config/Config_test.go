package config

import (
	"fmt"
	"testing"
)

func TestConfig(t *testing.T) {
	var testConfigData = []byte(
		`
instanceLogPath: ./instanceslog
serverLogPath: ./serverlog
instances:
-
    destination : example1
    slaveId : 123
    masterAddress : 192.168.0.135
    masterPort : 3306
    masterJournalName : mysql-bin.000001
    masterPosition : 107
    dbUsername : root
    dbPassword : 123456
    defaultDbName : test
-
    destination : example1
    slaveId : 124
    masterAddress : 192.168.0.135
    masterPort : 3306
    masterJournalName : mysql-bin.000001
    masterPosition : 107
    dbUsername : root
    dbPassword : 123456
    defaultDbName : test`)

	cfg, err := ParseConfigData(testConfigData)
	if err != nil {
		t.Fatal(err)
	}

	var lt int = 3
	switch lt {
	case 1:
		{
			fmt.Println("1111111111111")
		}
	case 2:
		{
			fmt.Println("2222222222222")
		}
	case 3:
		{
			fmt.Println("3333333333333")
		}
	default:
		{
			fmt.Println("4444444444444")
		}
	}

	fmt.Println(cfg.InstanceLogPath)
	fmt.Println(cfg.ServerLogPath)
	fmt.Println(cfg)
}
