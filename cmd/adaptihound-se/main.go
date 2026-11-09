package main

import (
	"log"
	"path/filepath"
	"strings"

	adaptix "github.com/Adaptix-Framework/axc2"
)

const serviceName = "AdaptiHoundSE"

// Keep this interface independent of AdaptixServer internals. The running
// Teamserver supplies these exported methods through InitPlugin's first argument.
type Teamserver interface {
	TsDownloadList() (string, error)
	TsDownloadGet(fileId string) (adaptix.DownloadData, error)
	TsDownloadGetFilepath(fileId string) (string, error)
	TsDownloadAdd(agentId string, fileId string, fileName string, fileSize int64) error
	TsDownloadUpdate(fileId string, state int, data []byte) error
	TsDownloadClose(fileId string, reason int) error
	TsDownloadDelete(fileIds []string) error
	TsServiceSendDataClient(operator string, service string, data string)
	TsExtenderDataSave(extenderName string, key string, value []byte) error
	TsExtenderDataLoad(extenderName string, key string) ([]byte, error)
}

// InitPlugin is the entry point required by Adaptix's Service Extender loader.
func InitPlugin(ts any, moduleDir string, serviceConfig string) adaptix.PluginService {
	server, ok := ts.(Teamserver)
	if !ok {
		log.Printf("%s: server does not provide the required Downloads/service API", serviceName)
		return nil
	}
	if strings.TrimSpace(serviceConfig) != "" {
		log.Printf("%s: service_config must be empty", serviceName)
		return nil
	}
	service, err := newService(server, filepath.Join(moduleDir, ".work"))
	if err != nil {
		log.Printf("%s: initialization failed: %v", serviceName, err)
		return nil
	}
	return service
}
