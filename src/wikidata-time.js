import { DateTime } from 'luxon';

export function wikidataToLuxon (timeObj) {
	const [date, time] = timeObj.time.split('T');
	const firstRealDash = date.indexOf('-', 1);

	const timeBits = time.substring(0, time.length - 1).split(':');

	const dateTimeObj = {
		year: parseInt(date.substring(0, firstRealDash), 10),
		month: parseInt(date.substring(firstRealDash + 1, firstRealDash + 3), 10) || undefined,
		day: parseInt(date.substring(firstRealDash + 4), 10) || undefined,

		hour: parseInt(timeBits[0], 10),
		minute: parseInt(timeBits[1], 10),
		second: parseInt(timeBits[2], 10)
	};

	let timezone = 'utc';
	if (timeObj.timezone !== 0) {
		const offset = timeObj.timezone / 60;
		let offsetStr;
		if (offset === Math.floor(offset)) {
			offsetStr = offset.toString();
		} else {
			offsetStr = offset.toFixed(1);
		}
		timezone = 'UTC+' + offsetStr;
	}

	return DateTime.fromObject(dateTimeObj, {
		zone: timezone
	});
}

export function areQualifiersWithinBounds (qualifiers, bounds = DateTime.now()) {
	if (!qualifiers) { return true; }

	// Check if it has ended
	if (qualifiers.P582) {
		const end = wikidataToLuxon(qualifiers.P582[0].datavalue.value);
		if (end < DateTime.now()) { return false; }
	}

	// Check if it has started
	if (qualifiers.P580) {
		const start = wikidataToLuxon(qualifiers.P580[0].datavalue.value);
		if (start > DateTime.now()) { return false; }
	}

	return true;
}
