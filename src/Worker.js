const { PrismaClient } = require('@prisma/client');
const { EventEmitter } = require('events');
const { fork } = require('child_process');
const path = require('path');
const os = require('os');

class Worker extends EventEmitter {
  constructor(queue, processor, options = {}) {
    super();
    this.queue = queue;
    this.processor = processor;
    this.options = {
      concurrency: 1,
      maxJobTime: 30000, // 30 seconds
      useChildProcess: true,
      backoff: {
        type: 'exponential',
        delay: 1000
      },
      transactionTimeout: 10000, // 10 seconds
      ...options
    };
    this.prisma = new PrismaClient();
    this.workerId = `worker-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
    this.running = false;
    this.childProcesses = new Map();
    this.processingJobs = 0;
    
    // Listen for cancel events from the queue
    this.queue.on('cancel', (jobId, workerId) => {
      if (workerId === this.workerId) {
        this.handleJobCancellation(jobId);
      }
    });
  }

  /**
   * Start the worker
   */
  async start() {
    if (this.running) return;
    this.running = true;
    await this.poll();
  }

  /**
   * Stop the worker
   */
  async stop() {
    this.running = false;
    // Kill all child processes
    for (const [jobId] of this.childProcesses) {
      this.killChildProcess(jobId);
    }
    await this.prisma.$disconnect();
  }

  /**
   * Poll for jobs
   */
  async poll() {
    if (!this.running) return;
    
    if (this.processingJobs < this.options.concurrency) {
      try {
        const nextJob = await this.queue.getNextJob();
        
        if (nextJob) {
          this.processingJobs++;
          await this.processJob(nextJob);
        }
      } catch (error) {
        this.emit('error', error);
      }
    }
    
    // Continue polling with a slight delay to prevent CPU spinning
    setTimeout(() => this.poll(), 100);
  }

  /**
   * Process a job
   */
  async processJob(job) {
    const timeout = setTimeout(() => {
      this.handleJobTimeout(job.id);
    }, this.options.maxJobTime);

    try {
      if (this.options.useChildProcess) {
        await this.processInChildProcess(job);
      } else {
        await this.processInMainProcess(job);
      }
      
      clearTimeout(timeout);
    } catch (error) {
      clearTimeout(timeout);
      await this.handleJobError(job, error);
    }
  }

  /**
   * Process job in child process
   */
  async processInChildProcess(job) {
    return new Promise((resolve, reject) => {
      const child = fork(path.join(__dirname, 'processor.js'));
      this.childProcesses.set(job.id, child);

      child.on('message', async (message) => {
        switch (message.type) {
          case 'progress':
            await this.updateProgress(job.id, message.progress);
            break;
          case 'complete':
            await this.completeJob(job.id, message.result);
            this.childProcesses.delete(job.id);
            child.kill();
            resolve();
            break;
          case 'error':
            this.childProcesses.delete(job.id);
            child.kill();
            reject(new Error(message.error));
            break;
        }
      });

      child.on('error', (error) => {
        this.childProcesses.delete(job.id);
        reject(error);
      });

      child.on('exit', (code) => {
        if (code !== 0) {
          reject(new Error(`Child process exited with code ${code}`));
        }
      });

      // Send job to child process
      child.send({
        job: {
          id: job.id,
          name: job.name,
          data: JSON.parse(job.data)
        },
        processor: this.processor.toString()
      });
    });
  }

  /**
   * Process job in main process
   */
  async processInMainProcess(job) {
    // Parse the job data safely
    let parsedData;
    try {
      parsedData = typeof job.data === 'string' ? JSON.parse(job.data) : job.data;
    } catch (error) {
      parsedData = job.data; // fallback to raw data if parsing fails
    }

    const result = await this.processor({
      id: job.id,
      name: job.name,
      data: parsedData,
      progress: async (progress) => {
        await this.updateProgress(job.id, progress);
      }
    });

    await this.completeJob(job.id, result);
  }

  /**
   * Update job progress
   */
  async updateProgress(jobId, progress) {
    await this.prisma.job.update({
      where: { id: jobId },
      data: { progress }
    });
    this.emit('progress', jobId, progress);
  }

  /**
   * Complete a job
   */
  async completeJob(jobId, result) {
    // Ensure result is properly stringified
    const stringifiedResult = typeof result === 'string' ? result : JSON.stringify(result);
    
    await this.prisma.job.update({
      where: { id: jobId },
      data: {
        status: 'completed',
        finishedAt: new Date(),
        result: stringifiedResult,
        progress: 100
      }
    });
    
    this.processingJobs--;
    this.emit('completed', jobId, result);
  }

  /**
   * Handle job error
   */
  async handleJobError(job, error) {
    const attempts = job.attempts + 1;
    const maxAttempts = job.maxAttempts;
    
    if (attempts < maxAttempts) {
      const delay = this.calculateBackoff(attempts);
      
      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: 'delayed',
          delay,
          attempts,
          error: error.message,
          workerId: null
        }
      });
      
      this.emit('failed', job.id, error, true);
    } else {
      await this.prisma.job.update({
        where: { id: job.id },
        data: {
          status: 'failed',
          finishedAt: new Date(),
          error: error.message,
          workerId: null
        }
      });
      
      this.emit('failed', job.id, error, false);
    }
    
    this.processingJobs--;
  }

  /**
   * Calculate backoff delay
   */
  calculateBackoff(attempts) {
    const { type, delay } = this.options.backoff;
    
    switch (type) {
      case 'exponential':
        return delay * Math.pow(2, attempts - 1);
      case 'linear':
        return delay * attempts;
      default:
        return delay;
    }
  }

  /**
   * Handle job timeout
   */
  async handleJobTimeout(jobId) {
    this.killChildProcess(jobId);
    await this.handleJobError(
      await this.prisma.job.findUnique({ where: { id: jobId } }),
      new Error('Job timeout')
    );
  }

  /**
   * Handle job cancellation
   */
  async handleJobCancellation(jobId) {
    this.killChildProcess(jobId);
    this.processingJobs--;
  }

  /**
   * Kill child process
   */
  killChildProcess(jobId) {
    const child = this.childProcesses.get(jobId);
    if (child) {
      child.kill();
      this.childProcesses.delete(jobId);
    }
  }
}

module.exports = Worker;
