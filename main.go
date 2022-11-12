package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"sync"
	"time"
)

func connectToDB() (*sql.DB, error) {
	open, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		"localhost", 5432, "postgres", "qwerty", "postgres"))
	if err != nil {
		return nil, err
	}
	return open, nil
}

func main() {
	var wgp sync.WaitGroup
	for i := 0; i < 1; i++ {
		wgp.Add(1)
		go func() {
			{
				var wg sync.WaitGroup
				db, err := connectToDB()
				if err != nil {
					fmt.Println("problem in connect")
					return
				}
				start := time.Now()
				for i := 0; i < 10; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := 0; i < 10000; i++ {
							tx, err := db.Begin()
							if err != nil {
								fmt.Println("problem in start")
								return
							}
							var counter int
							err = tx.QueryRow("select counter from counter WHERE user_id = 1").Scan(&counter)
							if err != nil {
								fmt.Println("problem in select")
								tx.Rollback()
								return
							}
							counter++
							_, err = tx.Exec("update counter set counter = $1 where user_id = 1", counter)
							if err != nil {
								fmt.Println("problem in exec")
								tx.Rollback()
								return
							}
							tx.Commit()
						}
					}()
				}
				wg.Wait()
				end := time.Now().Sub(start)
				fmt.Printf("time to finish lost update\": %v \n", end)
			}
		}()

		go func() {
			{
				var wg sync.WaitGroup
				db, err := connectToDB()
				if err != nil {
					fmt.Println("problem in connect")
					return
				}
				start := time.Now()
				for i := 0; i < 10; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := 0; i < 10000; i++ {
							tx, err := db.Begin()
							if err != nil {
								fmt.Println("problem in start")
								return
							}
							var counter int
							err = tx.QueryRow("select counter from counter WHERE user_id = 3 for update").Scan(&counter)
							if err != nil {
								fmt.Println("problem in select")
								tx.Rollback()
								return
							}
							counter++
							_, err = tx.Exec("update counter set counter = $1 where user_id = 3", counter)
							if err != nil {
								fmt.Println("problem in exec")
								tx.Rollback()
								return
							}
							tx.Commit()
						}
					}()
				}
				wg.Wait()
				end := time.Now().Sub(start)
				fmt.Printf("time to finish row-level locking: %v \n", end)
			}
		}()

		go func() {
			{
				var wg sync.WaitGroup
				db, err := connectToDB()
				if err != nil {
					fmt.Println("problem in connect")
					return
				}
				start := time.Now()
				for i := 0; i < 10; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := 0; i < 10000; i++ {
							tx, err := db.Begin()
							if err != nil {
								fmt.Println("problem in start")
								return
							}
							_, err = tx.Exec("update counter set counter = counter+1 where user_id = 2")
							if err != nil {
								fmt.Println("problem in exec")
								tx.Rollback()
								return
							}
							tx.Commit()
						}
					}()
				}
				wg.Wait()
				end := time.Now().Sub(start)
				fmt.Printf("time to finish in-place update: %v \n", end)
			}
		}()

		go func() {
			{
				var wg sync.WaitGroup
				db, err := connectToDB()
				if err != nil {
					fmt.Println("problem in connect")
					return
				}
				start := time.Now()
				for i := 0; i < 10; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for j := 0; j < 10000; j++ {
							flag := true
							for flag {
								tx, err := db.Begin()
								if err != nil {
									fmt.Println("problem in start")
									return
								}
								var counter int
								var version int
								err = tx.QueryRow("select counter,version from counter WHERE user_id = 4").Scan(&counter, &version)
								if err != nil {
									fmt.Println("problem in select")
									tx.Rollback()
									return
								}
								counter++
								res, err := tx.Exec("update counter set counter = $1,version=$2 where user_id = 4 and version=$3", counter, version+1, version)
								if err != nil {
									fmt.Println("problem in exec")
									tx.Rollback()
									return
								}
								tx.Commit()
								affected, err := res.RowsAffected()
								if err != nil {
									return
								}
								if affected > 0 {
									break
								}
							}

						}
					}()
				}
				wg.Wait()
				end := time.Now().Sub(start)
				fmt.Printf("time to finish optimistic concurrency control: %v \n", end)
			}
		}()
		wgp.Wait()
	}
}
