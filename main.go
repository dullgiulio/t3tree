package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const (
	queryPages   = "SELECT uid,pid,is_siteroot FROM pages"
	queryDomains = "SELECT pid,domainName,forced FROM sys_domain ORDER BY sorting ASC"
)

type mysql struct {
	db      *sql.DB
	pages   map[int]int      // uid : pid
	domains map[int]string   // pid : domain
	assoc   map[int][]string // pid : associated data
	roots   []int            // uid of siteroot
}

func newMysql(dsn string) (*mysql, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	m := &mysql{
		db:      db,
		pages:   make(map[int]int),
		domains: make(map[int]string),
		assoc:   make(map[int][]string),
		roots:   make([]int, 0),
	}
	if err := m.loadPages(); err != nil {
		return nil, err
	}
	if err := m.loadDomains(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *mysql) loadPages() error {
	rows, err := m.db.Query(queryPages)
	if err != nil {
		return err
	}
	for rows.Next() {
		var (
			pid, uid int
			isroot   bool
		)
		if err := rows.Scan(&uid, &pid, &isroot); err != nil {
			return fmt.Errorf("cannot read pages row: %v", err)
		}
		m.pages[uid] = pid
		if isroot || pid == 0 {
			m.roots = append(m.roots, uid)
		}
	}
	return nil
}

func (m *mysql) loadDomains() error {
	rows, err := m.db.Query(queryDomains)
	if err != nil {
		return err
	}
	for rows.Next() {
		var (
			pid    int
			domain string
			forced bool
		)
		if err := rows.Scan(&pid, &domain, &forced); err != nil {
			return fmt.Errorf("cannot read domains row: %v", err)
		}
		if _, ok := m.domains[pid]; ok { // && !forced {
			continue
		}
		m.domains[pid] = domain
	}
	return nil
}

func (m *mysql) query(sql string, nassoc int) ([]int, error) {
	rows, err := m.db.Query(sql)
	if err != nil {
		return nil, err
	}
	uids := make([]int, 0)
	assoc := make([]*string, nassoc)
	for i := 0; i < nassoc; i++ {
		b := ""
		assoc[i] = &b
	}
	params := make([]interface{}, nassoc+1)
	for rows.Next() {
		var uid int
		params[0] = &uid
		for i := 0; i < nassoc; i++ {
			params[i+1] = interface{}(assoc[i])
		}
		if err := rows.Scan(params...); err != nil {
			return nil, fmt.Errorf("cannot scan query: %v", err)
		}
		data := make([]string, nassoc)
		for i := 0; i < nassoc; i++ {
			data[i] = *assoc[i]
		}
		m.assoc[uid] = data
		uids = append(uids, uid)
	}
	return uids, nil
}

func (m *mysql) isRoot(pid int) bool {
	for i := range m.roots {
		if m.roots[i] == pid {
			return true
		}
	}
	return false
}

func (m *mysql) children(pid int, pids []int) []int {
	if pids == nil {
		pids = make([]int, 0)
	}
	for uid := range m.pages {
		if m.pages[uid] == pid {
			pids = append(pids, uid)
			pids = m.children(uid, pids)
		}
	}
	return pids
}

func (m *mysql) root(pid int) int {
	if m.isRoot(pid) {
		return pid
	}
	var ok bool
	for {
		pid, ok = m.pages[pid]
		if !ok {
			return 0
		}
		if m.isRoot(pid) {
			return pid
		}
	}
}

func (m *mysql) domain(pid int) string {
	return m.domains[pid]
}

func intsToString(a []int, sep string) string {
	if len(a) == 0 {
		return ""
	}

	b := make([]string, len(a))
	for i, v := range a {
		b[i] = strconv.Itoa(v)
	}
	return strings.Join(b, sep)
}

func main() {
	pid := flag.Int("pid", 0, "Page ID")
	dsn := flag.String("dsn", "", "Database connection string")
	query := flag.String("query", "", "A select that yield a list of page IDs")
	nassoc := flag.Int("nfields", 0, "Number of fields selected except page.uid")
	children := flag.Bool("children", false, "Select children pages")
	roots := flag.Bool("roots", false, "Select root pages")
	csv := flag.Bool("csv", false, "Show CSV for pids, for uid IN (...) query")
	flag.Parse()
	if *dsn == "" {
		log.Fatal("must have DSN as argument")
	}
	m, err := newMysql(*dsn)
	if err != nil {
		log.Fatalf("mysql error: %v", err)
	}
	var uids []int
	if *pid > 0 {
		if *children {
			uids = m.children(*pid, nil)
		}
		if *roots {
			uids = append(uids, m.root(*pid))
		}
		if !*children && !*roots {
			uids = append(uids, *pid)
		}
	}
	if *query != "" {
		qids, err := m.query(*query, *nassoc)
		if err != nil {
			log.Fatalf("cannot execute argument query: %v", err)
		}
		if *children {
			for _, qid := range qids {
				uids = m.children(qid, uids)
			}
		}
		if *roots {
			for _, qid := range qids {
				uids = append(uids, m.root(qid))
			}
		}
		if !*children && !*roots {
			uids = qids
		}
	}
	if len(uids) == 0 {
		log.Fatal("no UIDs found")
	}
	if *csv {
		fmt.Printf("%s\n", intsToString(uids, ", "))
	} else {
		fields := make([]string, *nassoc+1)
		for _, uid := range uids {
			rid := m.root(uid)
			domain := m.domain(rid)
			if domain == "" {
				continue
			}
			if *nassoc > 0 {
				assoc := m.assoc[uid]
				fields[0] = fmt.Sprintf("\"https://%s/index.php?id=%d\"", domain, uid)
				for i := range assoc {
					fields[i+1] = fmt.Sprintf("\"%s\"", strings.Replace(assoc[i], "\"", "\\\"", -1))
				}
				fmt.Printf("%s\n", strings.Join(fields, ","))
			} else {
				fmt.Printf("https://%s/index.php?id=%d\n", domain, uid)
			}
		}
	}
}
