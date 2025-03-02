const QueueFactory = require('../src/QueueFactory');
const Worker = require('../src/Worker');

async function startWorker() {
  const queue = await QueueFactory.createQueue('testQueue');
  
  // Define processor that will run in child process
  const processor = async (job) => {
    console.log('Child process starting job:', job.id);
    console.log('Process ID:', process.pid);
    
    // Simulate some work
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    // This should only exit the child process, not the main process
    console.log('Child process exiting...');
    process.exit(0);
    
    // This line should never be reached
    return { completed: true };
  };

  const worker = new Worker(queue, processor, {
    concurrency: 1,
    useChildProcess: true  // Make sure we use child process
  });

  // Add event listeners to see what happens
  worker.on('error', (error) => console.error('Worker error:', error));
  worker.on('completed', (jobId, result) => console.log('Job completed:', jobId, result));
  worker.on('failed', (jobId, error) => console.error('Job failed:', jobId, error));

  console.log('Main process ID:', process.pid);
  console.log('Starting worker...');
  await worker.start();

  // Add a test job
  const job = await queue.add('testJob', { test: 'data' });
  console.log('Added job:', job.id);

  // Keep main process running
  process.on('SIGINT', async () => {
    console.log('\nShutting down worker...');
    await worker.stop();
    await queue.close();
    process.exit(0);
  });
}

startWorker().catch(console.error);