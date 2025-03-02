const { EventEmitter } = require('events');
const cron = require('node-cron');

class Queue extends EventEmitter {
  constructor(name, options = {}) {
    super();
    this.name = name;
    this.options = {
      ...options
    };
    this.prisma = options.prisma;
    if (!this.prisma) {
      throw new Error('Prisma client instance is required');
    }
    this.paused = false;
    this.repeatableJobs = new Map();
  }

  /**
   * Add a job to the queue
   * @param {string} jobName - Name of the job
   * @param {object} data - Job data
   * @param {object} options - Job options including repeat settings
   * @returns {Promise<Job>} - The created job
   */
  async add(jobName, data, options = {}) {
    try {
      const now = new Date();
      const delay = options.delay || 0;
      const status = delay > 0 ? 'delayed' : 'pending';
      
      // Handle repeat options
      if (options.repeat) {
        return this.addRepeatableJob(jobName, data, options);
      }
      
      // Ensure data is properly stringified
      const stringifiedData = typeof data === 'string' ? data : JSON.stringify(data);
      
      return await this.prisma.job.create({
        data: {
          name: jobName,
          data: stringifiedData,
          status,
          delay,
          priority: options.priority || 0,
          maxAttempts: options.maxAttempts || 3,
          createdAt: now
        }
      });
    } catch (error) {
      this.emit('error', error);
      throw error;
    }
  }

  /**
   * Add a repeatable job
   * @private
   */
  async addRepeatableJob(jobName, data, options) {
    const { repeat } = options;
    const key = `${jobName}:${JSON.stringify(data)}:${JSON.stringify(repeat)}`;

    // If already scheduled, return existing
    if (this.repeatableJobs.has(key)) {
      return this.repeatableJobs.get(key).job;
    }

    let timer;
    let nextRun;
    let executionCount = 0;
    const limit = repeat.limit || Infinity;

    const createNewJob = async () => {
      const now = new Date();
      // Check if it's time to run the job
      if (nextRun && nextRun > now) {
        return;
      }

      executionCount++;
      
      // Stop if limit is reached
      if (executionCount >= limit) {
        if (timer.stop) {
          timer.stop();
        } else {
          clearInterval(timer);
        }
        this.repeatableJobs.delete(key);
        return;
      }

      // Create or update the job
      const job = await this.prisma.job.create({
        data: {
          name: jobName,
          data: typeof data === 'string' ? data : JSON.stringify(data),
          status: 'pending',
          priority: options.priority || 0,
          maxAttempts: options.maxAttempts || 3,
          createdAt: now
        }
      });

      // Update next run time
      if (repeat.cron) {
        nextRun = this.getNextCronRunTime(repeat.cron);
      } else if (typeof repeat === 'number') {
        nextRun = new Date(now.getTime() + repeat);
      } else if (repeat.every) {
        const ms = this.parseInterval(repeat.every);
        nextRun = new Date(now.getTime() + ms);
      }

      // Update the repeatable job record
      await this.prisma.repeatableJob.update({
        where: { id: repeatableJob.id },
        data: {
          nextRun,
          executionCount
        }
      });

      return job;
    };

    // Calculate initial nextRun time
    if (repeat.cron) {
      nextRun = this.getNextCronRunTime(repeat.cron);
    } else if (typeof repeat === 'number') {
      nextRun = new Date(Date.now() + repeat);
    } else if (repeat.every) {
      const ms = this.parseInterval(repeat.every);
      nextRun = new Date(Date.now() + ms);
    }

    // Create initial repeatable job record
    const repeatableJob = await this.prisma.repeatableJob.create({
      data: {
        name: jobName,
        queueName: this.name,
        data: JSON.stringify(data),
        options: JSON.stringify({ ...options, repeat: { ...repeat, limit } }),
        nextRun,
        pattern: typeof repeat === 'number' ? `${repeat}ms` : 
                repeat.cron ? repeat.cron : 
                repeat.every ? (typeof repeat.every === 'number' ? `${repeat.every}ms` : repeat.every) : '',
        limit,
        executionCount: 0
      }
    });

    // Don't create the initial job if it's a future cron job
    if (repeat.cron && nextRun > new Date()) {
      this.repeatableJobs.set(key, { timer, repeatableJob });
      return null;
    }

    // Create initial job
    const job = await this.prisma.job.create({
      data: {
        name: jobName,
        data: typeof data === 'string' ? data : JSON.stringify(data),
        status: 'pending',
        priority: options.priority || 0,
        maxAttempts: options.maxAttempts || 3,
        createdAt: new Date()
      }
    });

    this.repeatableJobs.set(key, { job, timer, repeatableJob });
    return job;
  }

  /**
   * Parse interval string or number to milliseconds
   * @private
   */
  parseInterval(interval) {
    // If interval is already a number, return it
    if (typeof interval === 'number') {
      return interval;
    }

    const units = {
      second: 1000,
      minute: 60 * 1000,
      hour: 60 * 60 * 1000,
      day: 24 * 60 * 60 * 1000,
      week: 7 * 24 * 60 * 60 * 1000
    };

    const [count, unit] = interval.split(' ');
    const baseUnit = unit.toLowerCase().replace(/s$/, '');
    
    if (!units[baseUnit]) {
      throw new Error(`Invalid interval unit: ${unit}`);
    }

    return parseInt(count) * units[baseUnit];
  }

  /**
   * Remove a repeatable job
   */
  async removeRepeatable(jobName, repeat) {
    const key = `${jobName}:${JSON.stringify(repeat)}`;
    const repeatable = this.repeatableJobs.get(key);
    
    if (repeatable) {
      if (repeatable.timer.stop) {
        repeatable.timer.stop();
      } else {
        clearInterval(repeatable.timer);
      }
      this.repeatableJobs.delete(key);

      await this.prisma.repeatableJob.delete({
        where: { id: repeatable.job.id }
      });
    }
  }

  /**
   * Get all repeatable jobs
   */
  async getRepeatableJobs() {
    return this.prisma.repeatableJob.findMany();
  }

  /**
   * Add a job with dependencies
   * @param {string} jobName - Name of the job
   * @param {object} data - Job data
   * @param {object} options - Job options including dependencies array
   * @returns {Promise<Job>} - The created job
   */
  async addWithDependencies(jobName, data, options = {}) {
    const { dependencies = [] } = options;
    
    const job = await this.add(jobName, data, options);
    
    if (dependencies.length > 0) {
      for (const depId of dependencies) {
        await this.prisma.jobDependency.create({
          data: {
            jobId: job.id,
            dependsOnId: depId
          }
        });
      }
      
      // Mark job as waiting if it has dependencies
      await this.prisma.job.update({
        where: { id: job.id },
        data: { status: 'waiting' }
      });
      
      this.emit('waiting', job);
    }
    
    return job;
  }

  /**
   * Add a child job
   * @param {number} parentId - ID of the parent job
   * @param {string} jobName - Name of the child job
   * @param {object} data - Job data
   * @param {object} options - Job options
   * @returns {Promise<Job>} - The created child job
   */
  async addChildJob(parentId, jobName, data, options = {}) {
    const childJob = await this.add(jobName, data, options);
    
    await this.prisma.jobRelation.create({
      data: {
        parentId,
        childId: childJob.id
      }
    });
    
    return childJob;
  }

  /**
   * Get the next available job
   * @returns {Promise<Job|null>} - The next job or null if none available
   */
  async getNextJob() {
    if (this.paused) return null;
    
    try {
      const now = new Date();
      
      // First try to get a pending or delayed job
      const job = await this.prisma.job.findFirst({
        where: {
          OR: [
            // Get pending jobs
            {
              status: 'pending',
              workerId: null
            },
            // Get delayed jobs where delay time has passed
            {
              status: 'delayed',
              workerId: null,
              AND: {
                createdAt: {
                  lte: new Date(now.getTime())
                }
              }
            }
          ]
        },
        orderBy: [
          { priority: 'desc' },
          { createdAt: 'asc' }
        ]
      });

      if (!job) return null;

      // For delayed jobs, check if enough time has passed
      if (job.status === 'delayed') {
        const delayMs = parseInt(job.delay, 10) || 0;
        const readyTime = new Date(job.createdAt.getTime() + delayMs);
        if (readyTime > now) {
          return null;
        }
      }

      // Update the job status to active
      return await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: 'active',
          processedAt: now,
          workerId: this.workerId
        }
      });
    } catch (error) {
      this.emit('error', error);
      return null;
    }
  }

  /**
   * Check for jobs with completed dependencies
   * @returns {Promise<void>}
   */
  async checkWaitingJobs() {
    const waitingJobs = await this.prisma.job.findMany({
      where: { status: 'waiting' },
      include: { dependencies: { include: { dependsOn: true } } }
    });
    
    for (const job of waitingJobs) {
      const allDependenciesCompleted = job.dependencies.every(
        dep => dep.dependsOn.status === 'completed'
      );
      
      if (allDependenciesCompleted) {
        await this.prisma.job.update({
          where: { id: job.id },
          data: { status: 'pending' }
        });
      }
    }
  }

  /**
   * Check for stalled jobs
   * @returns {Promise<void>}
   */
  async checkStalledJobs() {
    try {
      const stalledTimeout = 30000; // 30 seconds
      const now = new Date();
      const threshold = new Date(now.getTime() - stalledTimeout);

      await this.prisma.$transaction(async (prisma) => {
        const stalledJobs = await prisma.job.findMany({
          where: {
            status: 'active',
            processedAt: {
              lte: threshold
            },
            stalledAt: null
          }
        });

        for (const job of stalledJobs) {
          if (job.attempts < job.maxAttempts) {
            await prisma.job.update({
              where: { id: job.id },
              data: {
                status: 'pending',
                attempts: { increment: 1 },
                stalledAt: now,
                workerId: null
              }
            });
            this.emit('stalled', job);
          } else {
            await prisma.job.update({
              where: { id: job.id },
              data: {
                status: 'failed',
                error: 'Job stalled too many times',
                finishedAt: now
              }
            });
            this.emit('failed', job, new Error('Job stalled too many times'));
          }
        }
      }, {
        timeout: this.options.transactionTimeout
      });
    } catch (error) {
      this.emit('error', error);
    }
  }

  /**
   * Pause the queue
   * @returns {Promise<void>}
   */
  async pause() {
    this.paused = true;
    this.emit('paused');
  }

  /**
   * Resume the queue
   * @returns {Promise<void>}
   */
  async resume() {
    this.paused = false;
    this.emit('resumed');
  }

  /**
   * Get a job by ID
   * @param {number} jobId - The job ID
   * @returns {Promise<Job|null>} - The job or null if not found
   */
  async getJob(jobId) {
    return this.prisma.job.findUnique({ 
      where: { id: jobId },
      include: {
        dependencies: true,
        dependents: true,
        children: { include: { child: true } },
        parent: { include: { parent: true } }
      }
    });
  }

  /**
   * Get jobs by status
   * @param {string|string[]} status - Status or array of statuses
   * @param {number} offset - Pagination offset
   * @param {number} limit - Pagination limit
   * @returns {Promise<Job[]>} - Array of jobs
   */
  async getJobs(status, offset = 0, limit = 100) {
    const statuses = Array.isArray(status) ? status : [status];
    
    return this.prisma.job.findMany({ 
      where: { status: { in: statuses } },
      orderBy: { createdAt: 'desc' },
      skip: offset,
      take: limit
    });
  }

  /**
   * Count jobs by status
   * @param {string|string[]} status - Status or array of statuses
   * @returns {Promise<number>} - Count of jobs
   */
  async getJobCounts(status) {
    const statuses = Array.isArray(status) ? status : [status];
    
    return this.prisma.job.count({
      where: { status: { in: statuses } }
    });
  }

  /**
   * Remove a job
   * @param {number} jobId - The job ID to remove
   * @returns {Promise<void>}
   */
  async removeJob(jobId) {
    await this.prisma.job.delete({ where: { id: jobId } });
    this.emit('removed', jobId);
  }

  /**
   * Cancel a job
   * @param {number} jobId - The job ID to cancel
   * @param {string} reason - Reason for cancellation
   * @param {boolean} removeOnCancel - Whether to remove the job after cancellation
   * @returns {Promise<Job>} - The cancelled job
   */
  async cancelJob(jobId, reason = 'manually cancelled', removeOnCancel = false) {
    // Find the job first to check its current status
    const job = await this.prisma.job.findUnique({
      where: { id: jobId }
    });
    
    if (!job) {
      throw new Error(`Job with ID ${jobId} not found`);
    }
    
    // Only cancel jobs that aren't already completed, failed or cancelled
    if (['completed', 'failed', 'cancelled'].includes(job.status)) {
      return job;
    }
    
    // If the job is active, we need to handle the worker
    if (job.status === 'active' && job.workerId) {
      // Emit event to notify worker to stop processing
      this.emit('cancel', jobId, job.workerId);
    }
    
    if (removeOnCancel) {
      await this.prisma.job.delete({
        where: { id: jobId }
      });
      this.emit('removed', jobId);
      return { ...job, status: 'cancelled', cancelledAt: new Date() };
    } else {
      const updatedJob = await this.prisma.job.update({
        where: { id: jobId },
        data: {
          status: 'cancelled',
          cancelledAt: new Date(),
          finishedAt: new Date(),
          error: reason
        }
      });
      
      this.emit('cancelled', updatedJob);
      return updatedJob;
    }
  }

  /**
   * Cancel multiple jobs by their status
   * @param {string|string[]} status - Status or array of statuses to cancel
   * @param {number} limit - Maximum number of jobs to cancel
   * @param {boolean} removeOnCancel - Whether to remove jobs after cancellation
   * @returns {Promise<number>} - Number of cancelled jobs
   */
  async cancelJobs(status, limit = 1000, removeOnCancel = false) {
    const statuses = Array.isArray(status) ? status : [status];
    
    // Find jobs to cancel
    const jobsToCancel = await this.prisma.job.findMany({
      where: {
        status: { in: statuses }
      },
      take: limit
    });
    
    let cancelCount = 0;
    
    for (const job of jobsToCancel) {
      await this.cancelJob(job.id, 'Bulk cancellation', removeOnCancel);
      cancelCount++;
    }
    
    return cancelCount;
  }

  /**
   * Cancel jobs by name pattern
   * @param {string} pattern - Pattern to match job names
   * @param {boolean} removeOnCancel - Whether to remove jobs after cancellation
   * @returns {Promise<number>} - Number of cancelled jobs
   */
  async cancelJobsByPattern(pattern, removeOnCancel = false) {
    const jobs = await this.prisma.job.findMany({
      where: {
        name: {
          contains: pattern
        },
        status: {
          notIn: ['completed', 'failed', 'cancelled']
        }
      }
    });
    
    let cancelCount = 0;
    for (const job of jobs) {
      await this.cancelJob(job.id, `Bulk cancellation: ${pattern}`, removeOnCancel);
      cancelCount++;
    }
    
    return cancelCount;
  }

  /**
   * Clean old jobs
   * @param {number} grace - Age in milliseconds to keep jobs
   * @param {string|string[]} status - Status or array of statuses to clean
   * @returns {Promise<number>} - Number of cleaned jobs
   */
  async clean(grace, status = ['completed', 'failed']) {
    const statuses = Array.isArray(status) ? status : [status];
    const olderThan = new Date(Date.now() - grace);
    
    const result = await this.prisma.job.deleteMany({
      where: {
        status: { in: statuses },
        finishedAt: { lt: olderThan }
      }
    });
    
    this.emit('cleaned', result.count);
    return result.count;
  }

  /**
   * Close the queue
   * @returns {Promise<void>}
   */
  async close() {
    await this.prisma.$disconnect();
  }

  // Helper method to calculate next cron run time
  getNextCronRunTime(cronExpression) {
    const parts = cronExpression.split(' ');
    const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;
    
    const now = new Date();
    const nextRun = new Date(now);
    nextRun.setSeconds(0);
    nextRun.setMilliseconds(0);
    nextRun.setMinutes(parseInt(minute) || 0);
    nextRun.setHours(parseInt(hour) || 0);
    
    if (nextRun <= now) {
        nextRun.setDate(nextRun.getDate() + 1);
    }
    
    return nextRun;
  }
}

module.exports = Queue;
