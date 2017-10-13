package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/cihub/seelog"
)

/*
SignalFunc 自定义类型,表示信号的回调函数

参数s: 当前收到的信号
返回值isExit: 表示信号处理完成后是否退出程序
*/
type SignalFunc func(s os.Signal) (isExit bool)

func defaultSignalProcessor(s os.Signal) (isExit bool) {
	log.Warnf("Receive signal %s; Exit now.", s.String())
	isExit = true
	return isExit
}

// SignalHandler 信号处理类，管理程序要处理的信号
type SignalHandler struct {
	handlerMap map[os.Signal]SignalFunc
}

// NewSignalHandler 创建信号处理对象
func NewSignalHandler() *SignalHandler {
	sh := new(SignalHandler)
	sh.handlerMap = make(map[os.Signal]SignalFunc)

	// 默认要忽略的信号
	sh.Register(syscall.SIGWINCH, sh.ignore)
	sh.Register(syscall.SIGCHLD, sh.ignore)
	sh.Register(syscall.SIGCONT, sh.ignore)
	sh.Register(syscall.SIGURG, sh.ignore)
	sh.Register(syscall.SIGPIPE, sh.ignore)
	return sh
}

// Register 注册感兴趣的信号及该信号的回调函数
func (this *SignalHandler) Register(s os.Signal, f SignalFunc) {
	if _, exist := this.handlerMap[s]; !exist {
		if f == nil {
			this.handlerMap[s] = defaultSignalProcessor
		} else {
			this.handlerMap[s] = f
		}
	}
}

// UnRegister 解除已注册的信号及回调函数
func (this *SignalHandler) UnRegister(s os.Signal, f SignalFunc) {
	if _, exist := this.handlerMap[s]; exist {
		delete(this.handlerMap, s)
	}
}

// Start 开始对信号的拦截
func (this *SignalHandler) Start() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc) // 接收所有信号

	go func() {
		for {
			s := <-sc
			this.handle(s)
		}
	}()
}

func (this *SignalHandler) handle(s os.Signal) {
	if _, exist := this.handlerMap[s]; exist {
		if this.handlerMap[s](s) {
			log.Flush()
			os.Exit(0)
		}
	} else {
		log.Errorf("Not found signal(%s)'s handler, exit.", s.String())
		log.Flush()
		os.Exit(0)
	}
}

func (this *SignalHandler) ignore(s os.Signal) (isExit bool) {
	return false
}
