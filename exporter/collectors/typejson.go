package collectors

type Info struct {
	InfoTime string `json:"info_time"`
	Meta     struct {
		BeginTime          int    `json:"begin_time"`
		CompleteProportion int    `json:"complete_proportion"`
		Count              int    `json:"count"`
		Error              string `json:"error"`
		Followers          []struct {
			Node   string `json:"node"`
			Status string `json:"status"`
		} `json:"followers"`
		Leader struct {
			Node   string `json:"node"`
			Status string `json:"status"`
		} `json:"leader"`
	} `json:"meta"`
	Query struct {
		Detail []struct {
			Name       string `json:"name"`
			QPS        int    `json:"qps"`
			TotalQuery int    `json:"total_query"`
		} `json:"detail"`
		Error string `json:"error"`
	} `json:"query"`
	Space struct {
		Detail []struct {
			Detail []struct {
				Node   string `json:"node"`
				Remain int64  `json:"remain"`
				Used   int    `json:"used"`
			} `json:"detail"`
			Name string `json:"name"`
		} `json:"detail"`
		Error string `json:"error"`
	} `json:"space"`
}

//type Info struct {
//	InfoTime string `json:"info_time"`
//	Meta     struct {
//		Count  int    `json:"count"`
//		Master string `json:"master"`
//		Error  string `json:"error"`
//	} `json:"meta"`
//	Query struct {
//		Error  string `json:"error"`
//		Detail []struct {
//			Name       string `json:"name"`
//			QPS        int    `json:"qps"`
//			TotalQuery int    `json:"total_query"`
//		} `json:"detail"`
//	} `json:"query"`
//	Space struct {
//		Error  string `json:"error"`
//		Detail []struct {
//			Name   string `json:"name"`
//			Detail []struct {
//				Node   string `json:"node"`
//				Used   int    `json:"used"`
//				Remain int64  `json:"remain"`
//			} `json:"detail"`
//		} `json:"detail"`
//	} `json:"space"`
//}

type Checkup struct {
	CheckTime  string `json:"check_time"`
	Conclusion struct {
		Healthy string `json:"healthy"`
	} `json:"conclusion"`
	Epoch struct {
		DismatchNum int    `json:"dismatch_num"`
		Epoch       int    `json:"epoch"`
		Error       string `json:"error"`
		Result      string `json:"result"`
	} `json:"epoch"`
	Meta struct {
		Followers []struct {
			Node   string `json:"node"`
			Status string `json:"status"`
		} `json:"followers"`
		Leader struct {
			Node   string `json:"node"`
			Status string `json:"status"`
		} `json:"leader"`
		Result string `json:"result"`
	} `json:"meta"`
	Node struct {
		Count  int    `json:"count"`
		Down   int    `json:"down"`
		Error  string `json:"error"`
		Result string `json:"result"`
		Up     int    `json:"up"`
	} `json:"node"`
	Table struct {
		Count  int `json:"count"`
		Detail []struct {
			Incomplete   int    `json:"incomplete"`
			Inconsistent int    `json:"inconsistent"`
			Lagging      int    `json:"lagging"`
			Name         string `json:"name"`
			PCount       int    `json:"p-count"`
			Result       string `json:"result"`
			Slowdown     int    `json:"slowdown"`
			Stuck        int    `json:"stuck"`
		} `json:"detail"`
		Error string `json:"error"`
	} `json:"table"`
}

//type Checkup struct {
//	CheckTime  string `json:"check_time"`
//	Conclusion struct {
//		Healthy string `json:"healthy"`
//	} `json:"conclusion"`
//	Meta struct {
//		Result string `json:"result"`
//	} `json:"meta"`
//	Node struct {
//		Count  int    `json:"count"`
//		Up     int    `json:"up"`
//		Down   int    `json:"down"`
//		Result string `json:"result"`
//		Error  string `json:"error"`
//	} `json:"node"`
//	Epoch struct {
//		Epoch       int    `json:"epoch"`
//		DismatchNum int    `json:"dismatch_num"`
//		Result      string `json:"result"`
//		Error       string `json:"error"`
//	} `json:"epoch"`
//	Table struct {
//		Count  int    `json:"count"`
//		Error  string `json:"error"`
//		Detail []struct {
//			Name         string `json:"name"`
//			Result       string `json:"result"`
//			PCount       int    `json:"p-count,omitempty"`
//			Inconsistent int    `json:"inconsistent,omitempty"`
//			Incomplete   int    `json:"incomplete,omitempty"`
//			Lagging      int    `json:"lagging,omitempty"`
//		} `json:"detail"`
//	} `json:"table"`
//}
