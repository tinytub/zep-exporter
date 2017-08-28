package main

import (
	"github.com/tinytub/zep-exporter/cmd"
	"github.com/tinytub/zep-exporter/logging"
)

func main() {
	logging.Configure()
	cmd.Execute()
	//fmt.Println(connections["node"].Addrs)

	/*
		for _, nodeconn := range connections["node"].Conns["10.208.43.48:13223"] {
			fmt.Println(nodeconn.Conn)
		}
	*/

	/*
		fmt.Println("##############\n")

		for i := 1; i < 5; i++ {
			nodeconn := zeppelin.GetNodeConn("10.208.43.48:13223")
			fmt.Println(nodeconn.Conn)
		}

		fmt.Println(zeppelin.ListNode())
		tables, _ := zeppelin.ListTable()

		for _, table := range tables.GetName() {
			ptable, _ := zeppelin.PullTable(table)
			offset, _ := ptable.Offset(table)
			fmt.Println("offset: ", offset)
			server, _ := ptable.Server()
			fmt.Println("server: ", server)
			used, remain, _ := ptable.Space(table)
			fmt.Println("space: ", used, remain)
			query, qps, _ := ptable.Stats(table)
			fmt.Println("stats: ", query, qps)

			fmt.Println("tableEpoch: ", ptable.TableEpoch)
		}
	*/

	/*
		wg := &sync.WaitGroup{}

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup, i int) {
				fmt.Println(ListNode())
				wg.Done()
			}(wg, i)
			wg.Add(1)
			go func(wg *sync.WaitGroup, i int) {
				fmt.Println(ListMeta())
				wg.Done()
			}(wg, i)
		}

		time.Sleep(10 * time.Second)

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup, i int) {
				fmt.Println(ListNode())
				wg.Done()
			}(wg, i)
			wg.Add(1)
			go func(wg *sync.WaitGroup, i int) {
				fmt.Println(ListMeta())
				wg.Done()
			}(wg, i)
		}
		wg.Wait()
	*/
}
