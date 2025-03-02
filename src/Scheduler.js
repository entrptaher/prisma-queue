const cron = require('node-cron');

class Scheduler {
  constructor(queue) {
    this.queue = queue;
    this.jobs = new Map();
  }

  async addCronJob(jobName, cronExpression, data, options = {}) {
    // Validate cron expression
    if (!cron.validate(cronExpression)) {
      throw new Error('Invalid cron expression');
    }

    // Store in database
    const cronJob = await this.queue.prisma.cronJob.create({
      data: {
        jobName,
        queueName: this.queue.name,
        cronExpression,
        data: JSON.stringify(data),
        options: JSON.stringify(options),
        nextRun: this.getNextRun(cronExpression)
      }
    });

    // Schedule the job
    this.schedule(cronJob);
    
    return cronJob;
  }

  async start() {
    // Load all cron jobs for this queue
    const cronJobs = await this.queue.prisma.cronJob.findMany({
      where: { queueName: this.queue.name }
    });

    // Schedule each job
    for (const job of cronJobs) {
      this.schedule(job);
    }
  }

  schedule(cronJob) {
    const task = cron.schedule(cronJob.cronExpression, async () => {
      const data = JSON.parse(cronJob.data);
      const options = JSON.parse(cronJob.options);
      
      await this.queue.add(cronJob.jobName, data, options);
      
      // Update next run time
      await this.queue.prisma.cronJob.update({
        where: { id: cronJob.id },
        data: { nextRun: this.getNextRun(cronJob.cronExpression) }
      });
    });

    this.jobs.set(cronJob.id, task);
  }

  getNextRun(cronExpression) {
    return cron.schedule(cronExpression).nextDate().toDate();
  }

  async stop() {
    for (const task of this.jobs.values()) {
      task.stop();
    }
    this.jobs.clear();
  }
}

module.exports = Scheduler;