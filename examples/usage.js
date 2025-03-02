const QueueFactory = require('../src/QueueFactory');

async function addJobs() {
  try {
    console.log('Creating queue...');
    const queue = await QueueFactory.createQueue('myQueue', {
      databaseUrl: 'file:custom.db'
    });

    console.log('Adding jobs to queue...');
    
    // Add immediate job
    const job1 = await queue.add('testJob', {
      message: 'Immediate Task',
      timestamp: Date.now()
    });
    console.log('Added immediate job:', job1.id);

    // Add job with 10 second delay
    const job2 = await queue.add('testJob', {
      message: 'Delayed Task (10s)',
      timestamp: Date.now()
    }, { delay: 10000 }); // 10 seconds delay
    console.log('Added delayed job (10s):', job2.id);

    // Add job with 20 second delay
    const job3 = await queue.add('testJob', {
      message: 'Delayed Task (20s)',
      timestamp: Date.now()
    }, { delay: 20000 }); // 20 seconds delay
    console.log('Added delayed job (20s):', job3.id);

    // Close the queue connection
    await queue.close();
    console.log('Jobs added successfully');
    console.log('Note: Delayed jobs will be processed after their delay time has passed');
    process.exit(0);

  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

// Add some jobs
addJobs();
