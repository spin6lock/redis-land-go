package main
import (
    "log"
    "fmt"
    "io"
    "io/ioutil"
    "strconv"
    "os"
    "os/signal"
    "syscall"
    "flag"
    "encoding/json"
    "redis"
)

type Context struct {
    db    *Leveldb
    redis *redis.Redis
    m     *Monitor
    s     *Storer
    c     *CmdService
    quit_chan chan os.Signal
}

func usage() {
    fmt.Fprintf(os.Stderr, "usage: %s [config]\n", os.Args[0])
    flag.PrintDefaults()
    os.Exit(2)
}

type RedisConfig struct {
    Host string
    Password string
    Db string
    Events string
    Channel string
}

type LeveldbConfig struct {
    Dbname string
}

type Manager struct {
    Addr string
}

type Log struct {
    File string
}

type Zinc struct {
    Addr string
}

type Config struct {
    Redis []RedisConfig
    Leveldb LeveldbConfig
    Manager Manager
    Log Log
    Zinc Zinc
}

func main() {
    flag.Usage = usage
    flag.Parse()

    args := flag.Args()
    if len(args) < 1 {
        log.Panicf("config file is missing.")
    }

    var config Config
    content, err := ioutil.ReadFile(args[0])
    if err != nil {
        panic(err)
    }
    if err = json.Unmarshal([]byte(content), &config); err != nil {
        panic(err)
    }
    logfile := config.Log.File
    dbname := config.Leveldb.Dbname
    manager_addr := config.Manager.Addr
    zinc_addr := config.Zinc.Addr

    var host, password, events, channel string
    var db int
    for i, value := range config.Redis{
        redis_config := value
        host = redis_config.Host
        password = redis_config.Password
        tdb,_ := strconv.ParseInt(redis_config.Db, 0, 0)
        events = redis_config.Events
        channel = redis_config.Channel
        db = int(tdb)
    }

    fp,err := os.OpenFile(logfile, os.O_RDWR | os.O_APPEND | os.O_CREATE, 0666)
    if err != nil {
        log.Panicf("open log file failed:%s", err)
    }
    defer fp.Close()
    log.SetOutput(io.MultiWriter(fp, os.Stderr))


    queue := make(chan string, 1024)

    cli1 := redis.NewRedis(host, password, db)
    m := NewMonitor(cli1, events, channel)

    database := NewLeveldb()
    err = database.Open(dbname)
    if err != nil {
        log.Panicf("open db failed, err:%v", err)
    } else {
        log.Printf("open db succeed, dbname:%v", dbname)
    }
    defer database.Close()

    cli2 := redis.NewRedis(host, password, db)
    s := NewStorer(cli2, database)

    c := NewCmdService(manager_addr)

    cli3 := redis.NewRedis(host, password, db)
    context := &Context{database, cli3, m, s, c, make(chan os.Signal)}
    context.Register(c)

    zinc_agent := NewZincAgent(zinc_addr, database)

    signal.Notify(context.quit_chan, syscall.SIGINT, syscall.SIGTERM)

    go m.Start(queue)
    go s.Start(queue)
    go c.Start()
    go StartZincAgent(zinc_agent)

    log.Println("start succeed")
    log.Printf("catch signal %v, program will exit",<-context.quit_chan)
}

