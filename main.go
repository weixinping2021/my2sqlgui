package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	my "my2sql/base"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	xwidget "fyne.io/x/fyne/widget"
	"github.com/go-mysql-org/go-mysql/replication"
)

func clickBtn(user_inpurt *my.InputParameter) {
	my.GConfCmd.IfSetStopParsPoint = false
	//my.GConfCmd.setParameters(user_inpurt)

	if my.GConfCmd.WorkType != "stats" {
		my.G_HandlingBinEventIndex = &my.BinEventHandlingIndx{EventIdx: 1, Finished: false}
	}
	var wg, wgGenSql sync.WaitGroup
	wg.Add(1)
	go my.ProcessBinEventStats(my.GConfCmd, &wg)

	if my.GConfCmd.WorkType != "stats" {
		wg.Add(1)
		go my.PrintExtraInfoForForwardRollbackupSql(my.GConfCmd, &wg)
		for i := uint(1); i <= my.GConfCmd.Threads; i++ {
			wgGenSql.Add(1)
			go my.GenForwardRollbackSqlFromBinEvent(i, my.GConfCmd, &wgGenSql)
		}
	}
	if my.GConfCmd.Mode == "repl" {
		my.ParserAllBinEventsFromRepl(my.GConfCmd)
	} else if my.GConfCmd.Mode == "file" {
		myParser := my.BinFileParser{}
		myParser.Parser = replication.NewBinlogParser()
		// donot parse mysql datetime/time column into go time structure, take it as string
		myParser.Parser.SetParseTime(false)
		// sqlbuilder not support decimal type
		myParser.Parser.SetUseDecimal(false)
		myParser.MyParseAllBinlogFiles(my.GConfCmd)
	}
	wgGenSql.Wait()
	close(my.GConfCmd.SqlChan)
	wg.Wait()
}

func getAllDbName(cfg *my.ConfCmd) []string {
	rows, err := cfg.FromDB.Query("show databases")
	if err != nil {
		fmt.Println("error ocueer")
	}
	var dbName string
	var dbList []string
	for rows.Next() {
		rows.Scan(&dbName)
		dbList = append(dbList, dbName)
	}
	return dbList
}

func getAllTables(cfg *my.ConfCmd, dbname string) []string {
	rows, err := cfg.FromDB.Query("select table_name from information_schema.tables where table_schema='" + dbname + "'")
	if err != nil {
		fmt.Println("error ocueer")
	}
	var table string
	var tables []string
	for rows.Next() {
		rows.Scan(&table)
		tables = append(tables, table)
	}
	return tables
}

func main() {
	my.GConfCmd.IfSetStopParsPoint = false
	defer my.GConfCmd.CloseFH()

	app := app.NewWithID("weixp.newtools") // 创建应用程序实例
	window := app.NewWindow("my2sql")      // 创建窗口，标题为"my2sql"

	//离线模式窗口
	binlog := widget.NewEntry()
	binlog.SetPlaceHolder("选择要解析的binlog文件")
	binlog_bt := widget.NewButtonWithIcon("", theme.FileIcon(), func() {
		dialog.ShowFileOpen(func(f fyne.URIReadCloser, err error) {
			if err != nil {
				dialog.ShowError(err, window)
				return
			}
			saveFile := f.URI().Path()
			binlog.SetText(saveFile)
			my.GConfCmd.StartFile = saveFile
			my.GConfCmd.LocalBinFile = saveFile
		}, window)
	})

	container_binlog_bt := container.NewVBox(
		container.NewBorder(nil, nil, nil,
			container.NewHBox(
				container.NewVBox(layout.NewSpacer()),
				binlog_bt),
			binlog,
		),
		layout.NewSpacer(),
	)

	workDir := widget.NewEntry()
	workDir.SetPlaceHolder("选择解析文件目录")
	work_dir_bt := widget.NewButtonWithIcon("", theme.FolderOpenIcon(), func() {

		dialog.ShowFolderOpen(func(dir fyne.ListableURI, err error) {
			save_dir := "NoPathYet!"
			if err != nil {
				dialog.ShowError(err, window)
				return
			}
			if dir != nil {
				//fmt.Println(dir.Path())
				save_dir = dir.Path() // here value of save_dir shall be updated!
				workDir.SetText(save_dir)
				my.GConfCmd.OutputDir = save_dir
			}
		}, window)
	})

	container_work_dir := container.NewVBox(
		container.NewBorder(nil, nil, nil,
			container.NewHBox(
				container.NewVBox(layout.NewSpacer()),
				work_dir_bt),
			workDir,
		),
		layout.NewSpacer(),
	)

	db := widget.NewEntry()
	db.SetPlaceHolder("数据库名")

	table := widget.NewEntry()
	table.SetPlaceHolder("表名")

	tableDefinition := widget.NewMultiLineEntry()
	tableDefinition.SetPlaceHolder("表的创建语句,不要包括索引外键等信息")

	workType := widget.NewRadioGroup([]string{"原始sql", "回滚sql"}, func(value string) {
		if value == "原始sql" {
			my.GConfCmd.WorkType = "2sql"
		} else {
			my.GConfCmd.WorkType = "rollback"
		}
	})
	workType.Horizontal = true
	workType.SetSelected("原始sql")

	sqlTypes := widget.NewCheckGroup([]string{"insert", "update", "delete"}, func(value []string) {
		my.GConfCmd.FilterSql = value
		my.GConfCmd.FilterSqlLen = len(value)
	})
	sqlTypes.Horizontal = true
	sqlTypes.SetSelected([]string{"insert", "update", "delete"})

	//时间设置,默认是前1天的sql
	stopTime := widget.NewEntry()
	stopTime.SetText(time.Now().Format("2006-01-02 15:04:05"))

	startTime := widget.NewEntry()
	startTime.SetText(time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05"))
	container_time := container.New(layout.NewGridLayoutWithRows(1), startTime, stopTime)

	btn := widget.NewButton("开始分析", func() {
		my.GConfCmd.Mode = "file"
		//fmt.Println(my.GConfCmd.Mode)
		my.GConfCmd.OutputDir = workDir.Text
		my.GConfCmd.Databases = append([]string{}, db.Text)
		my.GConfCmd.Tables = append([]string{}, table.Text)
		my.GConfCmd.TableDef = tableDefinition.Text
		my.GConfCmd.ParseCmdOptions(startTime.Text, stopTime.Text)
		my.G_HandlingBinEventIndex = &my.BinEventHandlingIndx{EventIdx: 1, Finished: false}
		var wg, wgGenSql sync.WaitGroup
		wg.Add(1)
		go my.ProcessBinEventStats(my.GConfCmd, &wg)

		wg.Add(1)
		go my.PrintExtraInfoForForwardRollbackupSql(my.GConfCmd, &wg)
		for i := uint(1); i <= my.GConfCmd.Threads; i++ {
			wgGenSql.Add(1)
			go my.GenForwardRollbackSqlFromBinEvent(i, my.GConfCmd, &wgGenSql)
		}

		myParser := my.BinFileParser{}
		myParser.Parser = replication.NewBinlogParser()
		// donot parse mysql datetime/time column into go time structure, take it as string
		myParser.Parser.SetParseTime(false)
		// sqlbuilder not support decimal type
		myParser.Parser.SetUseDecimal(false)
		myParser.MyParseAllBinlogFiles(my.GConfCmd)
		wgGenSql.Wait()
		close(my.GConfCmd.SqlChan)
		wg.Wait()
		dialog.ShowInformation("my2sql", "解析完成", window)
	})

	container_offline := container.New(layout.NewVBoxLayout(), container_binlog_bt, container_work_dir, db, table, tableDefinition, container_time, workType, sqlTypes, btn)

	//在线模式的窗体
	container_work_dir_online := container.NewVBox(
		container.NewBorder(nil, nil, nil,
			container.NewHBox(
				work_dir_bt),
			workDir,
		),
		layout.NewSpacer(),
	)

	//添加在线的连接信息
	conName := widget.NewEntry()
	host := widget.NewEntry()
	host.SetPlaceHolder("127.0.0.1:3306")
	port := widget.NewEntry()
	user := widget.NewEntry()
	password := widget.NewPasswordEntry()

	content := container.New(layout.NewFormLayout(),
		widget.NewLabel("连接名称"),
		conName,
		widget.NewLabel("数据库地址"),
		host,
		widget.NewLabel("端口"),
		port,
		widget.NewLabel("用户名"),
		user,
		widget.NewLabel("密码"),
		password,
	)

	//连接选择
	db_select := xwidget.NewCompletionEntry([]string{})
	table_select := xwidget.NewCompletionEntry([]string{})

	con_select := widget.NewSelect([]string{}, func(s string) {
		if s != "" {
			//当选择了连接的时候,刷新数据库列表
			my.GConfCmd.Host = app.Preferences().StringList(s)[0]
			my.GConfCmd.User = app.Preferences().StringList(s)[2]
			my.GConfCmd.Passwd = app.Preferences().StringList(s)[3]
			port, _ := strconv.Atoi(app.Preferences().StringList(s)[1])
			my.GConfCmd.Port = uint(port)
			my.GConfCmd.CreateDB()
			db_select.Options = getAllDbName(my.GConfCmd)
		}
	})

	con_select.PlaceHolder = "实例选择"

	customDialog1 := dialog.NewCustomConfirm("数据库添加", "添加", "取消", content, func(b bool) {
		if b == true {
			oldDbList := app.Preferences().StringList("db_lists")
			oldDbList = append(oldDbList, conName.Text)
			app.Preferences().SetStringList("db_lists", oldDbList)
			app.Preferences().SetStringList(conName.Text, []string{host.Text, port.Text, user.Text, password.Text})
			con_select.Options = oldDbList
			con_select.Selected = conName.Text
		}
	}, window)

	customDialog1.Resize(fyne.NewSize(300, 300))
	button1 := widget.NewButtonWithIcon("", theme.ContentAddIcon(), func() {
		customDialog1.Show()
	})

	delConBtn := widget.NewButtonWithIcon("", theme.ContentRemoveIcon(), func() {
		dialog.ShowConfirm("确定删除", "删除连接"+con_select.Selected, func(b bool) {
			var new_options []string
			options := app.Preferences().StringList("db_lists")
			for _, val := range options {
				if val == con_select.Selected {
					continue
				} else {
					new_options = append(new_options, val)
				}
			}
			con_select.ClearSelected()
			con_select.Options = new_options
			app.Preferences().SetStringList("db_lists", new_options)
			app.Preferences().RemoveValue(con_select.Selected)
		}, window)
	})

	lists := app.Preferences().StringList("db_lists")
	for _, item := range lists {
		con_select.Options = append(con_select.Options, item)
		//fmt.Println(app.Preferences().StringList(item))
	}

	db_select.OnChanged = func(s string) {
		db_select.Options = getAllDbName(my.GConfCmd)
		db_select.ShowCompletion()
		//这里需要连接数据库,获取所有的库名
		newOptions := []string{}
		for _, value := range db_select.Options {
			if strings.Contains(value, s) {
				newOptions = append(newOptions, value)
			}
		}
		// then show them
		db_select.SetOptions(newOptions)
		db_select.ShowCompletion()
		// 获取表名
		table_select.Options = getAllTables(my.GConfCmd, s)
	}

	table_select.OnChanged = func(s string) {

		newOptions := []string{}
		for _, value := range table_select.Options {
			if strings.Contains(value, s) {
				newOptions = append(newOptions, value)
			}
		}
		// then show them
		table_select.SetOptions(newOptions)
		table_select.ShowCompletion()
		// 获取表名
		table_select.Options = getAllTables(my.GConfCmd, db_select.Text)
	}

	container_dt := container.NewGridWithRows(1, db_select, table_select)

	btn_online := widget.NewButton("开始分析", func() {
		if workDir.Text == "" {
			dialog.ShowInformation("错误提示", "必须设置工作目录", window)
			return
		}

		my.GConfCmd.Mode = "repl"
		//fmt.Println(my.GConfCmd.Mode)
		my.GConfCmd.OutputDir = workDir.Text
		my.GConfCmd.Databases = append([]string{}, db.Text)
		my.GConfCmd.Tables = append([]string{}, table.Text)
		my.GConfCmd.ParseCmdOptions(startTime.Text, stopTime.Text)
		my.G_HandlingBinEventIndex = &my.BinEventHandlingIndx{EventIdx: 1, Finished: false}
		var wg, wgGenSql sync.WaitGroup
		wg.Add(1)
		go my.ProcessBinEventStats(my.GConfCmd, &wg)

		wg.Add(1)
		go my.PrintExtraInfoForForwardRollbackupSql(my.GConfCmd, &wg)
		for i := uint(1); i <= my.GConfCmd.Threads; i++ {
			wgGenSql.Add(1)
			go my.GenForwardRollbackSqlFromBinEvent(i, my.GConfCmd, &wgGenSql)
		}

		myParser := my.BinFileParser{}
		myParser.Parser = replication.NewBinlogParser()
		// donot parse mysql datetime/time column into go time structure, take it as string
		myParser.Parser.SetParseTime(false)
		// sqlbuilder not support decimal type
		myParser.Parser.SetUseDecimal(false)
		myParser.MyParseAllBinlogFiles(my.GConfCmd)
		wgGenSql.Wait()
		close(my.GConfCmd.SqlChan)
		wg.Wait()
		dialog.ShowInformation("my2sql", "解析完成", window)
	})

	container_online := container.New(layout.NewVBoxLayout(), container.NewHBox(con_select, button1, delConBtn), container_work_dir_online, container_dt, container_time, workType, sqlTypes, btn_online)

	tabs := container.NewAppTabs(
		container.NewTabItem("在线模式", container_online),
		container.NewTabItem("离线模式", container_offline),
	)
	window.SetContent(tabs)
	window.Resize(fyne.NewSize(600, 400))
	window.ShowAndRun()

	//设置中文,不然会出现乱码
	err := os.Unsetenv("FYNE_FONT")
	if err != nil {
		return
	}
}
