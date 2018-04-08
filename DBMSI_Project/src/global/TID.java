package global;

public class TID {

	public int numRIDs;
	public int position;	
	public RID[] recordIDs;
	
	public TID() {
		// TODO Auto-generated constructor stub
	}

	public TID(int numRIDs) {

		this.numRIDs = numRIDs;
	}

	public TID(int numRIDs, int position) {

		this.numRIDs = numRIDs;
		this.position = position;
	}

	public TID(int numRIDs, int position, RID[] recordIDs) {

		this.numRIDs = numRIDs;
		this.position = position;
		this.recordIDs = recordIDs;	
	}

	public void copyTID (TID tid) {	

		this.numRIDs = tid.numRIDs;
		this.position = tid.position;
		this.recordIDs = tid.recordIDs;

	}

	public boolean equals(TID tid) {

		int i=0;
		
		if(this.numRIDs == tid.numRIDs && this.position == tid.position)
		{
			for(RID rid: recordIDs)
			{
				if(tid.recordIDs[i]!=null)
				{
					if(rid.slotNo == tid.recordIDs[i].slotNo && rid.pageNo.pid == tid.recordIDs[i++].pageNo.pid)
						continue;
					else
						return false;
				}
				else
					return false;
			}
			return true;
		}
		return false;
	}

	public void writeToByteArray(byte [] ary, int offset) {
		try{
			Convert.setIntValue ( numRIDs, offset, ary);
			Convert.setIntValue ( position, offset+4, ary);
			offset=offset+8;
			for(RID rec: recordIDs) {
				Convert.setIntValue ( rec.slotNo, offset, ary);
				Convert.setIntValue ( rec.pageNo.pid, offset+4, ary);
				offset=offset+8;
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	public int getPosition() {
		return position;
	}
	
	public void setPosition(int position) {
		this.position = position;
	}
	
	public void setRID(int column, RID recordID) {
		this.recordIDs[column-1]=recordID;
	}

	
}
