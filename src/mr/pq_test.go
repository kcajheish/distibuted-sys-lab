package mr

import (
	"container/heap"
	"testing"
	"time"
)

func TestPriorityQueue(t *testing.T) {
	nowUnixTime := time.Now().UnixMicro()

	type fields struct {
		tasks []Task
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		want   []int // task number in order
	}{
		{
			name: "basic",
			fields: fields{
				tasks: []Task{
					{
						TaskNumber: 1,
						JobType:    REDUCE,
						UnixTime:   nowUnixTime + 10,
						Status:     IN_PROGRESS,
					},
					{
						TaskNumber: 2,
						JobType:    MAP,
						UnixTime:   nowUnixTime + 10,
						Status:     IN_PROGRESS,
					},
					{
						TaskNumber: 3,
						JobType:    MAP,
						UnixTime:   nowUnixTime,
						Status:     IN_PROGRESS,
					},
					{
						TaskNumber: 4,
						JobType:    REDUCE,
						UnixTime:   nowUnixTime + 100,
						Status:     COMPLETED,
					},
				},
			},
			want: []int{4, 3, 2, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq := PriorityQueue{}
			initTasks := make([]*Task, 0, len(tt.fields.tasks))
			pq.tasks = initTasks
			for _, task := range tt.fields.tasks {
				heap.Push(&pq, &task)
			}

			for _, taskNumber := range tt.want {
				task := heap.Pop(&pq).(*Task) // pop function return any; thus we have to cast Task type here
				if task.TaskNumber != taskNumber {
					t.Errorf("want %d, got %d", taskNumber, task.TaskNumber)
				}
			}
		})
	}
}
