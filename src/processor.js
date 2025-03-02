process.on('message', async ({ job, processor }) => {
  try {
    // Create progress function
    const progress = (value) => {
      process.send({ type: 'progress', progress: value });
    };

    // Execute the processor
    const fn = eval(`(${processor})`);
    const result = await fn({ ...job, progress });

    // Send success message
    process.send({ type: 'complete', result });
  } catch (error) {
    // Send error message
    process.send({ type: 'error', error: error.message });
  }
});