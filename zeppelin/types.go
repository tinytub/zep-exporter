package zeppelin

type LNodes struct {
	Nodes []struct {
		Node struct {
			IP   string `json:"ip"`
			Port int    `json:"port"`
		} `json:"node"`
		Status int `json:"status"`
	} `json:"nodes"`
}
