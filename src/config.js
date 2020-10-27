import {readEnvironmentVariable} from '@natlibfi/melinda-backend-commons/';

// Api client variables
export const apiUrl = readEnvironmentVariable('API_URL');
export const apiUsername = readEnvironmentVariable('API_USERNAME');
export const apiPassword = readEnvironmentVariable('API_PASSWORD');
export const apiClientUserAgent = readEnvironmentVariable('API_CLIENT_USER_AGENT', {defaultValue: '_RECORD-UPDATE-HANDLER'});

// Mongo variables to job
export const mongoUrl = readEnvironmentVariable('MONGO_URI', {defaultValue: 'mongodb://127.0.0.1:27017/db'});
