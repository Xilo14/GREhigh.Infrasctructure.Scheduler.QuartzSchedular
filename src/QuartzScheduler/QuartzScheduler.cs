using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using GREhigh.DomainBase;
using GREhigh.DomainBase.Interfaces;
using GREhigh.Infrastructure.Interfaces;
using GREhigh.InfrastructureBase.Interfaces;
using GREhigh.Utility;
using Quartz;
using Quartz.Impl;

namespace GREhigh.Infrastructure.QuartzScheduler {
    public class QuartzScheduler : Interfaces.IScheduler {
        internal ConcurrentDictionary<string, long> roomIdsDict = new();
        internal ConcurrentDictionary<string, Type> roomTypesDict = new();
        private Quartz.IScheduler _schedular;
        private UpdateRoomProducer _updateRoomProducer;
        private Quartz.IScheduler Scheduler {
            get {
                if (_schedular == null)
                    Init();
                return _schedular;
            }
        }
        private void Init() {
            var factory = new StdSchedulerFactory();
            _schedular = factory.GetScheduler().Result;
            _schedular.Start().Wait();
        }
        private QuartzScheduler() { }
        public static QuartzScheduler Instance {
            get { return Singleton<QuartzScheduler>.Instance; }
            private set { }
        }
        public void RemoveJob(object jobId) {
            if (jobId is TriggerKey key) {
                Scheduler.UnscheduleJob(key);
            }
        }
        public object AddJobCancellation(TimeSpan timeout, long roomId, Type roomType) {
            var guid = Guid.NewGuid();
            roomIdsDict.TryAdd(guid.ToString(), roomId);
            roomTypesDict.TryAdd(guid.ToString(), roomType);

            var job = JobBuilder.Create<CancellationJob>()
                .WithIdentity(guid.ToString(), "CancellationJob")
                .UsingJobData("guid", guid.ToString())
                .Build();

            var trigger = TriggerBuilder.Create()
                .WithIdentity(guid.ToString(), "maintrigger")
                .StartAt(DateTime.Now + timeout)
                .Build();

            Scheduler.ScheduleJob(job, trigger);
            return trigger.Key;
        }
        public object AddJobFinishPreparing(TimeSpan timeout, long roomId, Type roomType) {
            var guid = Guid.NewGuid();
            roomIdsDict.TryAdd(guid.ToString(), roomId);
            roomTypesDict.TryAdd(guid.ToString(), roomType);

            var job = JobBuilder.Create<FinishPreparingJob>()
                .WithIdentity(guid.ToString(), "FinishPreparingJob")
                .UsingJobData("guid", guid.ToString())
                .Build();

            var trigger = TriggerBuilder.Create()
                .WithIdentity(guid.ToString(), "maintrigger")
                .StartAt(DateTime.Now + timeout)
                .Build();

            Scheduler.ScheduleJob(job, trigger);
            return trigger.Key;
        }
        public object AddJobTick(TimeSpan timeout, long roomId, Type roomType) {
            var guid = Guid.NewGuid();
            roomIdsDict.TryAdd(guid.ToString(), roomId);
            roomTypesDict.TryAdd(guid.ToString(), roomType);

            var job = JobBuilder.Create<TickJob>()
                .WithIdentity(guid.ToString(), "TickJob")
                .UsingJobData("guid", guid.ToString())
                .Build();

            var trigger = TriggerBuilder.Create()
                .WithIdentity(guid.ToString(), "maintrigger")
                .StartAt(DateTime.Now + timeout)
                .Build();

            Scheduler.ScheduleJob(job, trigger);
            return trigger.Key;
        }

        public void SetUpdateProducer(IProducer<IUpdateRoom> producer) {
            if (producer is UpdateRoomProducer)
                _updateRoomProducer = (UpdateRoomProducer)producer;
        }

        private class FinishPreparingJob : IJob {
            public async Task Execute(IJobExecutionContext context) {
                var dataMap = context.JobDetail.JobDataMap;
                var guid = dataMap.GetString("guid");
                Instance.roomIdsDict.TryGetValue(guid, out var roomId);
                Instance.roomTypesDict.TryGetValue(guid, out var roomType);
                Instance._updateRoomProducer.ProduceFinishPreparing(roomId, roomType);
            }
        }
        private class TickJob : IJob {
            public async Task Execute(IJobExecutionContext context) {
                var dataMap = context.JobDetail.JobDataMap;
                var guid = dataMap.GetString("guid");
                Instance.roomIdsDict.TryGetValue(guid, out var roomId);
                Instance.roomTypesDict.TryGetValue(guid, out var roomType);
                Instance._updateRoomProducer.ProduceTick(roomId, roomType);
            }
        }
        private class CancellationJob : IJob {
            public async Task Execute(IJobExecutionContext context) {
                var dataMap = context.JobDetail.JobDataMap;
                var guid = dataMap.GetString("guid");
                Instance.roomIdsDict.TryGetValue(guid, out var roomId);
                Instance.roomTypesDict.TryGetValue(guid, out var roomType);
                Instance._updateRoomProducer.ProduceCancellation(roomId, roomType);
            }
        }
    }
}
