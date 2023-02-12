package config

type Config struct {
	dataPath string
}

func NewConfig(path string) *Config {
	return &Config{
		dataPath: path,
	}
}

func (c Config) DataPath() string {
	return c.dataPath
}