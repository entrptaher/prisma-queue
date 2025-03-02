const QueueFactory = require('../src/QueueFactory');
const Worker = require('../src/Worker');

async function startWorker() {
  try {
    console.log('Creating queue...');
    const queue = await QueueFactory.createQueue('myQueue');

    // Define the processor function
    const processor = async (job) => {
      console.log('Processing repeatable job:', job.id);
      console.log('Job name:', job.name);
      console.log('Job data:', job.data);
      
      switch (job.name) {
        case 'repeatJob':
          console.log('Processing job that repeats every 5 seconds');
          break;
        case 'cronJob':
          console.log('Processing daily midnight job');
          break;
        case 'intervalJob':
          console.log('Processing 2-hour interval job');
          break;
      }
      
      // Simulate some work
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      return { processed: true, timestamp: new Date() };
    };

    console.log('Creating worker...');
    const worker = new Worker(queue, processor, {
      concurrency: 2, // Process 2 jobs at a time
      useChildProcess: false // For simplicity, run in main process
    });

    // Add event listeners
    worker.on('error', (error) => console.error('Worker error:', error));
    worker.on('completed', (jobId, result) => console.log('Job completed:', jobId, result));
    worker.on('failed', (jobId, error) => console.error('Job failed:', jobId, error));

    console.log('Starting worker...');
    await worker.start();
    
    console.log('Worker is running and waiting for repeatable jobs. Press Ctrl+C to exit.');

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
      console.log('\nShutting down worker...');
      await worker.stop();
      await queue.close();
      process.exit(0);
    });

  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

// Start the worker
startWorker();