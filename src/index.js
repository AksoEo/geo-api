import { getSubClasses } from './wikidata-sparql.js';
import { getWikiDataStream } from './wikidata-stream.js';

const humanSettlementClasses = [
	'Q486972',
	...await getSubClasses('Q486972')
];

const wikiDataStream = await getWikiDataStream();
wikiDataStream.on('data', function (obj) {
	const parentsArr = obj.claims.P31 || [];

	let isHumanSettlement = false;
	for (const parentObj of parentsArr) {
		const parentId = parentObj.mainsnak.datavalue.value.id;
		if (humanSettlementClasses.includes(parentId)) {
			isHumanSettlement = true;
			break;
		}
	}

	if (isHumanSettlement) {
		console.log(obj)
	}
});
