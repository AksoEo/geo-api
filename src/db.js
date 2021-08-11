import { promises as fs } from 'fs';
import Knex from 'knex';

export async function createDB (filename) {
	let exists = false;
	try {
		await fs.access(filename);
		exists = true;
	} catch (e) {
		// noop
	}
	if (exists) {
		throw new Error('db already exists');
	}

	const knex = Knex({
		client: 'sqlite3',
		connection: {
			filename
		},
		useNullAsDefault: true
	});

	await knex.schema.createTable('countries', function (table) {
		table.string('id').primary();
		table.string('iso', 2).index();
	});

	await knex.schema.createTable('cities', function (table) {
		table.string('id').primary();
		table.string('country').index().references('countries.iso');
	});

	return knex;
}
