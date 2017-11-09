package util

import (
	"bufio"
	"io"
	"strings"
	"os"
	"sync"
)

var ConfigHelperInstance *ConfigHelper = nil
var ConfigOnce sync.Once

type ConfigHelper struct {
	ConfigMap map[string]string
}

func InitConfig(path string)(c *ConfigHelper) {
	c = &ConfigHelper{ConfigMap:make(map[string]string)}
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		line, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		s := strings.TrimSpace(string(line))
		if strings.Index(s, "#") == 0 || len(s) == 0 {
			continue
		}

		n1 := strings.Index(s, "=")
		if n1 == len(s)-1 {
			continue
		}
		key := strings.TrimSpace(s[0:n1])
		value := strings.TrimSpace(s[n1+1 : len(s)])
		c.ConfigMap[key] = value
	}
	return
}

func NewConfigHelper() *ConfigHelper{
	ConfigOnce.Do(func() {
		ConfigHelperInstance = InitConfig("src/resources/adtarget.properties")
	})
	return ConfigHelperInstance
}


