import { google } from 'googleapis';

const auth = new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL,
  undefined,
  (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
  [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'
  ]
);

const drive = google.drive({ version: 'v3', auth });

// 可选：把新表直接创建到你共享好的文件夹里
const folderId = process.env.GOOGLE_FOLDER_ID || null;

const { data: file } = await drive.files.create({
  requestBody: {
    name: `权限测试表_${Date.now()}`,
    mimeType: 'application/vnd.google-apps.spreadsheet',
    parents: folderId ? [folderId] : undefined
  },
  fields: 'id'
});

console.log('created spreadsheetId:', file.id);
console.log('link:', `https://docs.google.com/spreadsheets/d/${file.id}`);
