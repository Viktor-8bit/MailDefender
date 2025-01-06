package schemas

const ScanSetting = `
{
	"type": "record",
	"name": "Settings",
	"fields": [
		{
			"name": "data",
			"type": {
				"type": "map",
				"values": "float"
			}
		}
	]
}`

const Url = `
{
	"type": "record",
	"name": "Url",
	"fields": [
		{
			"name": "url",
			"type": "string"
		}
	]
}`
