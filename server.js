import express from 'express';
import cors from 'cors';
import { ParquetReader } from '@dsnp/parquetjs';

const app = express();

app.use('/downloaded_images', express.static('downloaded_images'));
// Enable CORS for all origins (or customize if needed)
app.use(cors());
app.use(express.json()); // Middleware to parse JSON body

// app.use(cors({
//   origin: 'http://localhost:5001'
// }));


// 🔧 Utility to sanitize BigInt values
const sanitizeBigInts = obj =>
  JSON.parse(
    JSON.stringify(obj, (_, value) =>
      typeof value === 'bigint' ? value.toString() : value
    )
  );

app.post('/api/chat-shop-assistant', async (req, res) => {
  try {
    const { userMessage } = req.body;

    console.log('📂 Reading Parquet file...');
    const reader = await ParquetReader.openFile('all_channel_posts.parquet');
    const cursor = reader.getCursor();
    const results = [];

    let record;
    while ((record = await cursor.next())) {
      results.push(record);
    }

    await reader.close();
    console.log(`✅ Loaded ${results.length} records`);

    // 🔍 Optional filtering based on userMessage
    const filtered = userMessage
      ? results.filter(post =>
          post.title?.toLowerCase().includes(userMessage.toLowerCase()) ||
          post.description?.toLowerCase().includes(userMessage.toLowerCase())
        )
      : results;

    // 🧼 Sanitize BigInt values before sending
    const sanitized = filtered.map(sanitizeBigInts);

    res.json(sanitized);
  } catch (err) {
    console.error('Error reading Parquet:', err);
    res.status(500).json({ error: err.message || 'Failed to read Parquet file' });
  }
});

app.listen(5001, () => {
  console.log('🚀 API running at http://localhost:5001');
});
