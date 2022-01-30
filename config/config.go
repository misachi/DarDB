package config

type Config struct {
	dataPath string
}

func (c Config) DataPath() string {
	return c.dataPath
}