# SQLite Queue

A robust job queue system built on SQLite and Prisma, designed for Node.js applications that need reliable background job processing with minimal setup.

## Features

- Persistent job storage using SQLite
- Job scheduling with delays and priorities
- Job dependencies and parent-child relationships
- Cron job support
- Progress tracking and job lifecycle events
- Concurrent job processing
- Child process isolation
- Automatic job retries with configurable backoff
- Job cancellation and cleanup

## Installation

```bash
npm install sqlite-queue
# or
yarn add sqlite-queue
# or
pnpm add sqlite-queue
```

## Quick Start

### Adding Jobs

```javascript
const QueueFactory = require('sqlite-queue');

// Create a queue instance
const queue = await QueueFactory.createQueue('myQueue', {
  databaseUrl: 'file:./custom.db'
});

// Add a job
const job = await queue.add('emailJob', {
  to: 'user@example.com',
  subject: 'Welcome!'
});

// Add a delayed job
const delayedJob = await queue.add('reminderJob', {
  userId: 123,
  message: 'Remember to check your profile'
}, { delay: 3600000 }); // 1 hour delay
```

### Processing Jobs

```javascript
const Worker = require('sqlite-queue/worker');

// Create a worker
const worker = new Worker(queue, async (job) => {
  // Process job
  console.log('Processing:', job.data);
  
  // Update progress
  await job.progress(50);
  
  // Return result
  return { success: true };
}, {
  concurrency: 2
});

// Handle events
worker.on('completed', (jobId, result) => console.log('Job completed:', jobId));
worker.on('failed', (jobId, error) => console.error('Job failed:', jobId, error));

// Start processing
await worker.start();
```

### Scheduling Cron Jobs

```javascript
const scheduler = new Scheduler(queue);

// Add a cron job that runs every day at midnight
await scheduler.addCronJob(
  'dailyReport',
  '0 0 * * *',
  { type: 'report' },
  { priority: 1 }
);
```

## Configuration

### Queue Options

- `databaseUrl`: SQLite database URL (default: `file:./queue.db`)

### Worker Options

- `concurrency`: Number of concurrent jobs (default: 1)
- `maxJobTime`: Maximum job execution time in ms (default: 30000)
- `useChildProcess`: Run jobs in separate processes (default: true)
- `backoff`: Retry backoff configuration
  - `type`: 'exponential' or 'fixed' (default: 'exponential')
  - `delay`: Initial delay in ms (default: 1000)

## Job States

- `pending`: Waiting to be processed
- `active`: Currently being processed
- `completed`: Successfully processed
- `failed`: Failed processing
- `delayed`: Scheduled for future processing
- `cancelled`: Manually cancelled

## API Reference

### Queue

- `add(name, data, options)`: Add a new job
- `addWithDependencies(name, data, options)`: Add a job with dependencies
- `getJob(id)`: Get job by ID
- `getJobs(status, offset, limit)`: Get jobs by status
- `cancelJob(id)`: Cancel a job
- `clean(grace, status)`: Remove old jobs

### Worker

- `start()`: Start processing jobs
- `stop()`: Stop processing jobs
- `pause()`: Pause job processing
- `resume()`: Resume job processing

### Events

- `completed`: Job completed successfully
- `failed`: Job failed
- `progress`: Job progress updated
- `cancelled`: Job was cancelled
- `cleaned`: Old jobs were cleaned

## License

ISC