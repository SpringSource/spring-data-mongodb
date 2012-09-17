package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.DBRef;

public class PersonWithAddressDBRef {

	@Id
	String id;

	String name;

	@DBRef
	AddressWithPersonWithDBRef address;

	@DBRef
	List<AddressWithPersonWithDBRef> addresses = new ArrayList<AddressWithPersonWithDBRef>();

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PersonWithAddressDBRef other = (PersonWithAddressDBRef) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

}
