
const QueueFactory = require('../src/QueueFactory');
const Worker = require('../src/Worker');

async function startWorker() {
  try {
    console.log('Creating queue...');
    const queue = await QueueFactory.createQueue('myQueue', {
      databaseUrl: 'file:custom.db'
    });

    // Define the processor function
    const processor = async (job) => {
      console.log('Processing job:', job.id);
      console.log('Job data:', job.data);
      
      // Simulate some work
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // You can update progress if needed
      await job.progress(50);
      
      // Simulate more work
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      return { processed: true, completedAt: new Date() };
    };

    console.log('Creating worker...');
    const worker = new Worker(queue, processor, {
      concurrency: 1,
      useChildProcess: false
    });

    // Add event listeners
    worker.on('error', (error) => console.error('Worker error:', error));
    worker.on('completed', (jobId, result) => console.log('Job completed:', jobId, result));
    worker.on('failed', (jobId, error) => console.error('Job failed:', jobId, error));
    worker.on('progress', (jobId, progress) => console.log('Job progress:', jobId, progress));

    console.log('Starting worker...');
    await worker.start();
    
    console.log('Worker is running and waiting for jobs. Press Ctrl+C to exit.');

    // Handle process termination
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
