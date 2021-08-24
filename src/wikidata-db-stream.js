import { Writable } from 'stream';
import { DateTime } from 'luxon';

import { wikidataToLuxon, areQualifiersWithinBounds } from './wikidata-time.js';

function isSubClassOf (obj, classes) {
	const parentsArr = obj.claims.P31 || [];
	for (const parentObj of parentsArr) {
		const parentId = parentObj.mainsnak.datavalue.value.id;
		if (classes.includes(parentId)) {
			return true;
		}
	}
	return false;
}

export default class WikidataDBStream extends Writable {
	constructor (db, humanSettlementClasses, territorialEntityClasses) {
		super({
			objectMode: true
		});
		this.db = db;
		this.humanSettlementClasses = humanSettlementClasses;
		this.territorialEntityClasses = territorialEntityClasses;
	}

	async handleTerritorialEntity (obj) {
		await this.db('territorial_entities').insert({
			id: obj.id
		});

		const parents = obj.claims.P131 || [];
		for (const parent of parents) {
			if (!areQualifiersWithinBounds(parent.qualifiers)) {
				continue;
			}
			await this.db('territorial_entities_parents')
				.insert({
					id: obj.id,
					parent: parent.mainsnak.datavalue.value.id
				});
		}
	}

	async handleHumanSettlement (obj) {
		if (!obj.claims.P17) { return; } // we cannot use the entry without its country
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

		await this.db('cities').insert({
			id: obj.id,
			country: countryId,
			population
		});

		// Insert labels
		await this.db('cities_labels').insert(
			Object.values(obj.labels).map(labelObj => {
				return {
					id: obj.id,
					lang: labelObj.language,
					label: labelObj.value
				};
			})
		);

		// Insert native labels
		const nativeLabels = [];
		if (obj.claims.P1705) { // native label
			for (const claim of obj.claims.P1705) {
				nativeLabels.push({
					id: obj.id,
					lang: claim.mainsnak.datavalue.value.language,
					label: claim.mainsnak.datavalue.value.text,
					native_order: nativeLabels.length
				});
			}
		}
		if (obj.claims.P1448) { // official name
			for (const claim of obj.claims.P1448) {
				if (!areQualifiersWithinBounds(claim.qualifiers)) {
					continue;
				}
				nativeLabels.push({
					id: obj.id,
					lang: claim.mainsnak.datavalue.value.language,
					label: claim.mainsnak.datavalue.value.text,
					native_order: nativeLabels.length
				});
			}
		}

		if (nativeLabels.length) {
			await this.db('cities_labels').insert(nativeLabels);
		}
	}

	async _write (obj, encoding, next) {
		if (obj.claims.P297) {
			// It is a country as it has an ISO 3166-1 alpha-2 code
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
			// We do not exit here, as city-states are a thing
		}

		const isTerritorialEntity = isSubClassOf(obj, this.territorialEntityClasses);
		if (isTerritorialEntity) {
			await this.handleTerritorialEntity(obj);
		}

		const isHumanSettlement = isSubClassOf(obj, this.humanSettlementClasses);
		if (isHumanSettlement) {
			await this.handleHumanSettlement(obj);
		}

		next();
	}
}