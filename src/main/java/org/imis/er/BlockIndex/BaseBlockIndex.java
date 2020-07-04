package org.imis.er.BlockIndex;

import org.imis.calcite.adapter.csv.CsvEnumerator;
import org.imis.er.DataStructures.EntityProfile;

public class BaseBlockIndex extends BlockIndex{

	public void createBlockIndex(CsvEnumerator<Object[]> enumerator, Integer key)
	{
		while(enumerator.moveNext()) {
			//System.out.println(enumerator.current()[0].toString());
			Object[] currentLine = enumerator.current();
			Integer fields = currentLine.length;
			if(currentLine[key].toString().equals("")) continue;
			EntityProfile eP = new EntityProfile(currentLine[key].toString());
			int index = 0;
			while(index < fields) {
				if(index != key) {
					eP.addAttribute(index, currentLine[index].toString());;
				}
				index ++;
			}
			this.entityProfiles.add(eP);
		}
		enumerator.close();
	}

}
