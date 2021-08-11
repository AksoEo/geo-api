import { createDB } from './db.js';
import WikidataDBStream from './wikidata-db-stream.js';
import { getSubClasses } from './wikidata-sparql.js';
import { getWikidataStream } from './wikidata-stream.js';

console.log('Creating db ...')
const db = await createDB('./test.db');

console.log('Fetching subclasses of human settlement through SparQL')
const humanSettlementClasses = await getSubClasses('Q486972');
humanSettlementClasses.push('Q486972');

console.log('Creating a stream of the latest Wikidata database dump')
const wikidataStream = await getWikidataStream();

console.log('Populating db')
wikidataStream.pipe(new WikidataDBStream(db, humanSettlementClasses));
wikidataStream.on('finish', function (){
	db.destroy();
});
