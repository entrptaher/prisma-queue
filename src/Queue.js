const { EventEmitter } = require('events');

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
  }

  /**
   * Add a job to the queue
   * @param {string} jobName - Name of the job
   * @param {object} data - Job data
   * @param {object} options - Job options
   * @returns {Promise<Job>} - The created job
   */
  async add(jobName, data, options = {}) {
    try {
      const now = new Date();
      const delay = options.delay || 0;
      const status = delay > 0 ? 'delayed' : 'pending';
      
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
}

module.exports = Queue;
