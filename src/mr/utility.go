package mr

import (
	"fmt"
	"log"
)

const Debug = true

func IntermediateFileName(mid int, rid int) string {
	return fmt.Sprintf("mr-%v-%v", mid, rid)
}

func DPrintf(format string, val ...interface{}) {
	info := fmt.Sprintf(format+"\n", val...)
	if Debug {
		log.Printf(info)
	}
}