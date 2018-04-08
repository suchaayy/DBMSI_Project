package value;

public class IntegerValue extends ValueClass {

	private int value;

	public IntegerValue() {
	
	}
	
	public IntegerValue(int value) {
		this.value = value;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) { 
		this.value = value;
	}
}