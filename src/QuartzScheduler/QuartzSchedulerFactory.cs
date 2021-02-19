using GREhigh.Infrastructure.Interfaces;

namespace GREhigh.Infrastructure.QuartzScheduler {
    public class QuartzSchedulerFactory : IInfrastructureFactory<IScheduler> {
        public IScheduler GetInfrastructure() {
            return QuartzScheduler.Instance;
        }
    }
}
