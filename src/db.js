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

	await knex.schema.createTable('object_languages', function (table) {
		table.string('id');
		table.string('lang_id');
		table.primary(['id', 'lang_id']);
	});

	await knex.schema.createTable('languages', function (table) {
		table.string('id');
		table.string('code').index();
		table.primary(['id', 'code'])
	});

	await knex.schema.createTable('territorial_entities', function (table) {
		table.string('id').primary();
	});

	await knex.schema.createTable('territorial_entities_parents', function (table) {
		table.string('id');
		table.string('parent').index();
		table.primary(['id', 'parent']);
	});

	await knex.schema.createTable('cities', function (table) {
		table.string('id').primary();
		table.string('country').index();
		table.integer('population').index();
		table.decimal('lat', 5, 3).index();
		table.decimal('lon', 6, 3).index();
	});

	await knex.schema.createTable('cities_labels', function (table) {
		table.string('id');
		table.string('lang').index();
		table.integer('native_order').index();
		table.string('label');
		table.primary(['id', 'lang', 'native_order']);
	});

	return knex;
}
