package collectors

type Info struct {
	InfoTime string `json:"info_time"`
	Meta     struct {
		Count  int    `json:"count"`
		Master string `json:"master"`
		Error  string `json:"error"`
	} `json:"meta"`
	Query struct {
		Error  string `json:"error"`
		Detail []struct {
			Name       string `json:"name"`
			QPS        int    `json:"qps"`
			TotalQuery int    `json:"total_query"`
		} `json:"detail"`
	} `json:"query"`
	Space struct {
		Error  string `json:"error"`
		Detail []struct {
			Name   string `json:"name"`
			Detail []struct {
				Node   string `json:"node"`
				Used   int    `json:"used"`
				Remain int64  `json:"remain"`
			} `json:"detail"`
		} `json:"detail"`
	} `json:"space"`
}

type Checkup struct {
	CheckTime  string `json:"check_time"`
	Conclusion struct {
		Healthy string `json:"healthy"`
	} `json:"conclusion"`
	Meta struct {
		Result string `json:"result"`
	} `json:"meta"`
	Node struct {
		Count  int    `json:"count"`
		Up     int    `json:"up"`
		Down   int    `json:"down"`
		Result string `json:"result"`
		Error  string `json:"error"`
	} `json:"node"`
	Epoch struct {
		Epoch       int    `json:"epoch"`
		DismatchNum int    `json:"dismatch_num"`
		Result      string `json:"result"`
		Error       string `json:"error"`
	} `json:"epoch"`
	Table struct {
		Count  int    `json:"count"`
		Error  string `json:"error"`
		Detail []struct {
			Name         string `json:"name"`
			Result       string `json:"result"`
			PCount       int    `json:"p-count,omitempty"`
			Inconsistent int    `json:"inconsistent,omitempty"`
			Incomplete   int    `json:"incomplete,omitempty"`
			Lagging      int    `json:"lagging,omitempty"`
		} `json:"detail"`
	} `json:"table"`
}
