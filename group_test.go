/*
 * Copyright 2021 Wang Min Xiang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package workers_test

import (
	"context"
	"fmt"
	"github.com/aacfactory/workers"
	"testing"
)

type GroupTask struct {
	n int
}

func (task *GroupTask) Execute(ctx context.Context) (result interface{}, err error) {
	if task.n%2 == 0 {
		err = fmt.Errorf("foo")
	} else {
		result = ctx.Value("sss")
	}
	return
}

func TestGroup_Run(t *testing.T) {
	worker := workers.New()
	group := worker.Group()
	for i := 0; i < 10; i++ {
		group.Add(fmt.Sprintf("%d", i), &GroupTask{
			n: i,
		})
	}
	ctx := context.WithValue(context.TODO(), "sss", "sss")
	future := group.Run(ctx)
	result, err := future.Wait(ctx)
	if err != nil {
		t.Error("future", err)
		return
	}
	fmt.Println(result.Succeed(), result.Results(), result.Errors())
}
