import { Writable } from 'stream';
import { DateTime } from 'luxon';

import { wikidataToLuxon, areQualifiersWithinBounds } from './wikidata-time.js';

export default class WikidataDBStream extends Writable {
	constructor (db, classes) {
		super({
			objectMode: true
		});
		this.db = db;
		this.classes = classes;
	}

	async _write (obj, encoding, next) {
		const parentsArr = obj.claims.P31 || [];
		let isHumanSettlement = false;
		for (const parentObj of parentsArr) {
			const parentId = parentObj.mainsnak.datavalue.value.id;
			if (this.classes.includes(parentId)) {
				isHumanSettlement = true;
				break;
			}
		}

		if (!isHumanSettlement) {
			// We may still want to store it if it is a country
			if (!obj.claims.P297) { return next(); } // Must have ISO 3166-1 alpha-2 code
			let codeEntry;
			for (codeEntry of obj.claims.P297) {
				if (areQualifiersWithinBounds(obj.claims.P297.qualifiers)) {
					break;
				}
			}

			await this.db('countries').insert({
				id: obj.id,
				iso: codeEntry.mainsnak.datavalue.value.toLowerCase()
			});

			return next();
		}

		if (!obj.claims.P17) { return next(); } // we cannot use the entry without this data
		let countryId;
		for (const countryEntry of obj.claims.P17) {
			countryId = countryEntry.mainsnak.datavalue.value.id;
			if (areQualifiersWithinBounds(countryEntry.qualifiers)) {
				break;
			}
		}

		let population = null;
		let populationTime = Number.MIN_SAFE_INTEGER;
		if (obj.claims.P1082) {
			for (const populationEntry of obj.claims.P1082) {
				let newPopulationTime = Number.MIN_SAFE_INTEGER;
				if (populationEntry.qualifiers && populationEntry.qualifiers.P585) {
					newPopulationTime = wikidataToLuxon(populationEntry.qualifiers.P585[0].datavalue.value);
				}
				if (newPopulationTime >= populationTime) {
					population = parseInt(populationEntry.mainsnak.datavalue.value.amount, 10);
					populationTime = newPopulationTime;
				}
			}
		}

		console.log(population)

		await this.db('cities').insert({
			id: obj.id,
			country: countryId,
			population
		});

		await this.db('cities_labels').insert(
			Object.values(obj.labels).map(labelObj => {
				return {
					city: obj.id,
					lang: labelObj.language,
					label: labelObj.value
				};
			})
		);

		next();
	}
}