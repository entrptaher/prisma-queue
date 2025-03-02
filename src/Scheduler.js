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
      await this.runJob(cronJob);
    });

    this.jobs.set(cronJob.id, task);
  }

  async runJob(cronJob) {
    const data = JSON.parse(cronJob.data);
    const options = JSON.parse(cronJob.options);
    
    await this.queue.add(cronJob.jobName, data, options);
    
    // Update next run time
    await this.queue.prisma.cronJob.update({
      where: { id: cronJob.id },
      data: { nextRun: this.getNextRun(cronJob.cronExpression) }
    });
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

  // New methods for managing scheduled jobs

  /**
   * Get all scheduled jobs
   */
  async getJobs() {
    return this.queue.prisma.cronJob.findMany({
      where: { queueName: this.queue.name }
    });
  }

  /**
   * Get a specific scheduled job by ID
   */
  async getJob(jobId) {
    return this.queue.prisma.cronJob.findUnique({
      where: { id: jobId }
    });
  }

  /**
   * Manually run a scheduled job immediately
   */
  async runJobById(jobId) {
    const cronJob = await this.getJob(jobId);
    if (!cronJob) {
      throw new Error(`Scheduled job with ID ${jobId} not found`);
    }
    
    await this.runJob(cronJob);
    return cronJob;
  }

  /**
   * Remove a scheduled job
   */
  async removeJob(jobId) {
    // Stop the cron task if it's running
    const task = this.jobs.get(jobId);
    if (task) {
      task.stop();
      this.jobs.delete(jobId);
    }

    // Remove from database
    await this.queue.prisma.cronJob.delete({
      where: { id: jobId }
    });
  }

  /**
   * Pause a scheduled job
   */
  async pauseJob(jobId) {
    const task = this.jobs.get(jobId);
    if (task) {
      task.stop();
    }

    await this.queue.prisma.cronJob.update({
      where: { id: jobId },
      data: { paused: true }
    });
  }

  /**
   * Resume a paused scheduled job
   */
  async resumeJob(jobId) {
    const cronJob = await this.getJob(jobId);
    if (!cronJob) {
      throw new Error(`Scheduled job with ID ${jobId} not found`);
    }

    if (cronJob.paused) {
      this.schedule(cronJob);
      await this.queue.prisma.cronJob.update({
        where: { id: jobId },
        data: { paused: false }
      });
    }
  }
}

module.exports = Scheduler;
