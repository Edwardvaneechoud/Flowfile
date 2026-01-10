/**
 * Example demonstrating FlowFrame with operation graph tracking
 * Mirrors the Python FlowFrame style
 */

import { FlowFrame, col, lit, pl } from '../dist/index.js';

async function main() {
  console.log('=== FlowFile WASM - Basic Operations Example ===\n');

  // Create sample data
  const data = {
    name: ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank'],
    age: [25, 30, 35, 28, 32, 29],
    city: ['NYC', 'LA', 'NYC', 'Chicago', 'LA', 'NYC'],
    salary: [70000, 80000, 90000, 75000, 85000, 72000]
  };

  const df = pl.DataFrame(data);
  console.log('Original DataFrame:');
  console.log(df.toString());
  console.log('\n');

  // Create FlowFrame - this tracks all operations
  const ff = FlowFrame.fromDataFrame(df);

  // Example 1: Filter and select (like Python: df.filter(pl.col('age') > 28).select(['name', 'age']))
  console.log('Example 1: Filter age > 28 and select columns');
  const result1 = ff
    .filter(col('age').gt(28))
    .select(col('name'), col('age'));

  console.log(result1.collect().toString());
  result1.printGraph();

  // Example 2: Group by and aggregate (like Python: df.group_by('city').agg(pl.col('salary').mean()))
  console.log('Example 2: Group by city and calculate mean salary');
  const result2 = ff
    .groupBy('city')
    .agg(col('salary').mean().alias('avg_salary'));

  console.log(result2.collect().toString());
  result2.printGraph();

  // Example 3: Complex chain with multiple operations
  console.log('Example 3: Complex operation chain');
  const result3 = ff
    .filter(col('age').gte(28))
    .withColumns(
      col('salary').mul(1.1).alias('salary_with_bonus')
    )
    .select(col('name'), col('city'), col('salary_with_bonus'))
    .sort('salary_with_bonus', true);

  console.log(result3.collect().toString());
  result3.printGraph();

  // Example 4: Join two FlowFrames
  console.log('Example 4: Join operation');
  const cityData = pl.DataFrame({
    city: ['NYC', 'LA', 'Chicago'],
    state: ['NY', 'CA', 'IL']
  });

  const cityFF = FlowFrame.fromDataFrame(cityData);

  const result4 = ff
    .filter(col('age').gt(25))
    .join(cityFF, { on: 'city', how: 'left' })
    .select(col('name'), col('city'), col('state'), col('salary'));

  console.log(result4.collect().toString());
  result4.printGraph();

  // Example 5: String operations
  console.log('Example 5: String operations');
  const result5 = ff
    .filter(col('name').str.startsWith('A').or(col('name').str.startsWith('E')))
    .withColumns(
      col('name').str.toUppercase().alias('name_upper')
    )
    .select(col('name'), col('name_upper'), col('age'));

  console.log(result5.collect().toString());
  result5.printGraph();

  // Example 6: Serialize operation graph (for sending to visual designer)
  console.log('Example 6: Serialized operation graph (JSON)');
  const serialized = result3.serializeGraph();
  console.log(JSON.stringify(serialized, null, 2));

  console.log('\n=== All examples completed ===');
}

main().catch(console.error);
