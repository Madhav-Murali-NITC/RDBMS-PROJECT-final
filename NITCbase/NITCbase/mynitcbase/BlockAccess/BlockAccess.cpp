#include "BlockAccess.h"
#include <stdlib.h>
#include <stdio.h>
#include <cstring>


RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);		
    int block,slot;
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        RelCatEntry relCatBuf;
        //CHECK THIS PART DOUBT
        RelCacheTable::getRelCatEntry(relId, &relCatBuf);
        //DOUBT HERE AGAIN
        block = relCatBuf.firstBlk;
        slot = 0;
    }
    else
    {
	    block = prevRecId.block;
	    slot = prevRecId.slot+1;
    }
    
    RelCatEntry relCatBuff;
    RelCacheTable::getRelCatEntry(relId, &relCatBuff);
    while (block != -1)
    {
        /* create a RecBuffer object for block (use RecBuffer Constructor for
           existing block) */
      
        //CHECK DOUBT
    RecBuffer recBuff(block);
	HeadInfo head;
	
	recBuff.getHeader(&head);
	unsigned char* slotMap= (unsigned char*) malloc (head.numSlots*sizeof(unsigned char));
	recBuff.getSlotMap(slotMap);
        if(slot>= relCatBuff.numSlotsPerBlk)
        {
            block = head.rblock;
            slot=0;
            continue; 
        }
        if(slotMap[slot]==SLOT_UNOCCUPIED)
        {
            slot++;
            continue;
        }
	
	Attribute record[head.numAttrs];
	recBuff.getRecord(record, slot);
	AttrCatEntry attrCatBuf;
	AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatBuf);
	int offset= attrCatBuf.offset;
	Attribute attrAtOffset= record[offset];

        int cmpVal;  // will store the difference between the attributes
        // set cmpVal using compareAttrs()
        //printf("attrVal %0.2f attrvaaal %s\n", attrVal.nVal, attrVal.sVal);
        //printf("attrType %d\n", attrType);
	//cmpVal= compareAttrs(attrAtOffset,attrVal, attrType);
    cmpVal= compareAttrs(attrAtOffset,attrVal,attrCatBuf.attrType);
    //printf("cmpVal %d\n", cmpVal);
        if (
            (op == NE && cmpVal != 0) ||    // if op is "not equal to"
            (op == LT && cmpVal < 0) ||     // if op is "less than"
            (op == LE && cmpVal <= 0) ||    // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) ||    // if op is "equal to"
            (op == GT && cmpVal > 0) ||     // if op is "greater than"
            (op == GE && cmpVal >= 0)       // if op is "greater than or equal to"
        ) {
            RecId currentRec;
            currentRec.block=block;
            currentRec.slot=slot;
            RelCacheTable::setSearchIndex(relId, &currentRec);  

            return RecId{block, slot};
        }

        slot++;
    }

    // no record in the relation with Id relid satisfies the given condition
    return RecId{-1, -1};
}

int BlockAccess::renameRelation (char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName;
    strcpy(newRelationName.sVal, newName);
    char relationName[10];
    strcpy(relationName, "RelName");

    RecId searchNewRel = BlockAccess::linearSearch(RELCAT_RELID, relationName, newRelationName, EQ);
    if(searchNewRel.block != -1 && searchNewRel.slot != -1)
        return E_RELEXIST;

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName;
    strcpy(oldRelationName.sVal, oldName);

    RecId searchOldRel = BlockAccess::linearSearch(RELCAT_RELID, relationName, oldRelationName, EQ);

    if(searchOldRel.block == -1 && searchOldRel.slot == -1)
        return E_RELNOTEXIST;

    Attribute modifiedRecord[RELCAT_NO_ATTRS];
    RecBuffer recBuff(searchOldRel.block);
    recBuff.getRecord(modifiedRecord, searchOldRel.slot);

    strcpy(modifiedRecord[RELCAT_REL_NAME_INDEX].sVal, newName);

    recBuff.setRecord(modifiedRecord, searchOldRel.slot);
    
    // TO-DO Update all attribute catalog entries with newname

    RecId attributeCatRecord;
    Attribute modifiedAttributeRecord[ATTRCAT_NO_ATTRS];

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    while(true){

        attributeCatRecord = BlockAccess::linearSearch(ATTRCAT_RELID, relationName, oldRelationName, EQ);
        
        if(attributeCatRecord.block == -1 && attributeCatRecord.slot == -1)
            break;
        
        RecBuffer recBuff(attributeCatRecord.block);
        recBuff.getRecord(modifiedAttributeRecord, attributeCatRecord.slot);
        strcpy(modifiedAttributeRecord[ATTRCAT_REL_NAME_INDEX].sVal, newName);
        recBuff.setRecord(modifiedAttributeRecord, attributeCatRecord.slot);

    }

    return SUCCESS;

}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    //We check if the relation with name relName exists and return E_RELNOTEXIST if not.
    //We check if the attribute with name oldName exists in the relation with name relName and return E_ATTRNOTEXIST if not.
    //We check if the attribute with name newName exists in the relation with name relName and return E_ATTREXIST if so.
    //We then change the attribute name in the attribute catalog entry for the attribute with name oldName to newName.

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    char relationName[10];
    strcpy(relationName, "RelName");

    RecId searchRel = BlockAccess::linearSearch(RELCAT_RELID, relationName, relNameAttr, EQ);

    if(searchRel.block == -1 && searchRel.slot == -1)
        return E_RELNOTEXIST;

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    //This loop will find the attribute catalog entry for the attribute with name oldName and store its record id in attrToRenameRecId.

    while(true){

        RecId recId = BlockAccess::linearSearch(ATTRCAT_RELID, relationName, relNameAttr, EQ);
        
        if(recId.block == -1 && recId.slot == -1)
            break;
        
        RecBuffer recBuff(recId.block);
        recBuff.getRecord(attrCatEntryRecord, recId.slot);

        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0)
            attrToRenameRecId = {recId.block, recId.slot};

        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0)
            return E_ATTREXIST;
    }

    if(attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1)
        return E_ATTRNOTEXIST;

    RecBuffer recBuff(attrToRenameRecId.block);
    recBuff.getRecord(attrCatEntryRecord, attrToRenameRecId.slot);
    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
    recBuff.setRecord(attrCatEntryRecord, attrToRenameRecId.slot);

    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record){

  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(relId, &relCatEntry);

  int blockNum = relCatEntry.firstBlk;
  RecId rec_id = {-1, -1};

  int numAttrs = relCatEntry.numAttrs;
  int numSlots = relCatEntry.numSlotsPerBlk;

  int prevBlockNum = -1;
  int freeSlot = -1;
  int freeBlock = -1;

  while(blockNum != -1){

    RecBuffer recBuff(blockNum);
    HeadInfo head;
    recBuff.getHeader(&head);
    unsigned char* slotMap = (unsigned char*) malloc (head.numSlots*sizeof(unsigned char));
    recBuff.getSlotMap(slotMap);

    for(int slotIndex=0; slotIndex < numSlots; slotIndex++){
      if(slotMap[slotIndex] == SLOT_UNOCCUPIED){
        rec_id = {blockNum, slotIndex};
        break;
      }
    }
    if(rec_id.block != -1 && rec_id.slot != -1)
        break;

    prevBlockNum = blockNum;
    blockNum = head.rblock;

  }

  if(rec_id.block == -1 && rec_id.slot == -1){
    if(relId == RELCAT_RELID)
        return E_MAXRELATIONS;
    RecBuffer recBuff;
    blockNum = recBuff.getBlockNum();

    if(blockNum == E_DISKFULL)
        return E_DISKFULL;

    rec_id.block = blockNum;
    rec_id.slot = 0;

    HeadInfo head1;

    head1.blockType = REC;
    head1.pblock = -1;
    head1.numAttrs = numAttrs;
    head1.numSlots = numSlots;
    head1.numEntries = 0;
    head1.rblock = -1;

    if(blockNum != relCatEntry.firstBlk)
        head1.lblock = prevBlockNum;
    else
        head1.lblock = -1;
    
    recBuff.setHeader(&head1);
    unsigned char *slotMap = ((unsigned char*) malloc (numSlots*sizeof(unsigned char)));
    for(int i=0; i<numSlots; i++)
        slotMap[i] = SLOT_UNOCCUPIED;
    recBuff.RecBuffer::setSlotMap(slotMap);

    if(prevBlockNum != -1){
        RecBuffer prevRecBuff(prevBlockNum);
        HeadInfo head2;
        prevRecBuff.getHeader(&head2);
        head2.rblock = blockNum;
        prevRecBuff.setHeader(&head2);
    }
    else{
        relCatEntry.firstBlk = blockNum;
        RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }

  }

    RecBuffer recBuff2(rec_id.block);
    recBuff2.setRecord(record, rec_id.slot);
    unsigned char *slotMap = ((unsigned char*) malloc (numSlots*sizeof(unsigned char)));
    recBuff2.getSlotMap(slotMap);
    slotMap[rec_id.slot] = SLOT_OCCUPIED;
    recBuff2.setSlotMap(slotMap);
    
    HeadInfo blockHeader;
	recBuff2.getHeader(&blockHeader);
	blockHeader.numEntries++;
	recBuff2.setHeader(&blockHeader);

    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    int flag=SUCCESS;
    // Iterate over all the attributes of the relation
    // (let attrOffset be iterator ranging from 0 to numOfAttributes-1)
    int attrOffset = 0;
    while(attrOffset < numAttrs)
    {
        // get the attribute catalog entry for the attribute from the attribute cache
        // (use AttrCacheTable::getAttrCatEntry() with args relId and attrOffset)
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, attrOffset, &attrCatEntry);

        // get the root block field from the attribute catalog entry
        int rootBlock = attrCatEntry.rootBlock;
        if (rootBlock!=-1)
        {
            /* insert the new record into the attribute's bplus tree using
             BPlusTree::bPlusInsert()*/
            int retVal = BPlusTree::bPlusInsert(relId, attrCatEntry.attrName,
                                                record[attrOffset], rec_id);

            if (retVal == E_DISKFULL) {
                //(index for this attribute has been destroyed)
                // flag = E_INDEX_BLOCKS_RELEASED
                flag=E_INDEX_BLOCKS_RELEASED;
            }
        }
    }

    return flag;

}

int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // Declare a variable called recid to store the searched record
    RecId recId;

    /* get the attribute catalog entry from the attribute cache corresponding
    to the relation with Id=relid and with attribute_name=attrName  */
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // if this call returns an error, return the appropriate error code
    if (ret!=SUCCESS)
        return ret;

    // get rootBlock from the attribute catalog entry
    if(attrCatEntry.rootBlock==-1){

        /* search for the record id (recid) corresponding to the attribute with
           attribute name attrName, with value attrval and satisfying the
           condition op using linearSearch()
        */
        recId = linearSearch(relId, attrName, attrVal, op);
    }

    else{
        // (index exists for the attribute)

        /* search for the record id (recid) correspoding to the attribute with
        attribute name attrName and with value attrval and satisfying the
        condition op using BPlusTree::bPlusSearch() */
        recId = BPlusTree::bPlusSearch(relId, attrName, attrVal, op);
    }


    // if there's no record satisfying the given condition (recId = {-1, -1})
    //     return E_NOTFOUND;
    if(recId.block==-1 && recId.slot==-1)
        return E_NOTFOUND;

    /* Copy the record with record id (recId) to the record buffer (record).
       For this, instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */
    RecBuffer recBuff(recId.block);
    recBuff.getRecord(record, recId.slot);

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    // if the relation to delete is either Relation Catalog or Attribute Catalog,
    //     return E_NOTPERMITTED
        // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
        // you may use the following constants: RELCAT_NAME and ATTRCAT_NAME)
    if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
        return E_NOTPERMITTED;

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
       RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr; // (stores relName as type union Attribute)
    // assign relNameAttr.sVal = relName
    strcpy(relNameAttr.sVal, relName);

    //  linearSearch on the relation catalog for RelName = relNameAttr
    char RelName[8]="RelName";
    RecId relCatRecId = linearSearch(RELCAT_RELID,RelName , relNameAttr, EQ);

    // if the relation does not exist (linearSearch returned {-1, -1})
    //     return E_RELNOTEXIST
    if (relCatRecId.block==-1 && relCatRecId.slot==-1)
        return E_RELNOTEXIST;

    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
    /* store the relation catalog record corresponding to the relation in
       relCatEntryRecord using RecBuffer.getRecord */
       RecBuffer recBuff(relCatRecId.block);
        recBuff.getRecord(relCatEntryRecord, relCatRecId.slot);

    /* get the first record block of the relation (firstBlock) using the
       relation catalog entry record */
    /* get the number of attributes corresponding to the relation (numAttrs)
       using the relation catalog entry record */

    int firstBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
    int numAttrs = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;


    /*
     Delete all the record blocks of the relation
    */
    // for each record block of the relation:
    //     get block header using BlockBuffer.getHeader
    //     get the next block from the header (rblock)
    //     release the block usithe relang BlockBuffer.releaseBlock
    //
    //     Hint: to know if we reached the end, check if nextBlock = -1
    int currentBlock=firstBlock,nextBlock;
    while(currentBlock!=-1){

    BlockBuffer currentBlockbuf(currentBlock);
    HeadInfo head;
    currentBlockbuf.getHeader(&head);
    nextBlock=head.rblock;
    currentBlockbuf.releaseBlock();
    currentBlock=nextBlock;
    }



    /***
        Deleting attribute catalog entries corresponding the relation and index
        blocks corresponding to the relation with relName on its attributes
    ***/

    // reset the searchIndex of the attribute catalog

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    

    int numberOfAttributesDeleted = 0;

    while(true) {
        RecId attrCatRecId;
        // attrCatRecId = linearSearch on attribute catalog for RelName = relNameAttr
        
        attrCatRecId = linearSearch(ATTRCAT_RELID,RelName,relNameAttr, EQ);

        // if no more attributes to iterate over (attrCatRecId == {-1, -1})
        //     break;
        if(attrCatRecId.block==-1 && attrCatRecId.slot==-1)
            break;

        numberOfAttributesDeleted++;

        // create a RecBuffer for attrCatRecId.block
        // get the header of the block
        // get the record corresponding to attrCatRecId.slot
        RecBuffer recBuff(attrCatRecId.block);
        HeadInfo head;
        recBuff.getHeader(&head);
        Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];
        recBuff.getRecord(attrCatEntryRecord, attrCatRecId.slot);

        // declare variable rootBlock which will be used to store the root
        // block field from the attribute catalog record.

        int rootBlock = attrCatEntryRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
        // (This will be used later to delete any indexes if it exists)

        // Update the Slotmap for the block by setting the slot as SLOT_UNOCCUPIED
        // Hint: use RecBuffer.getSlotMap and RecBuffer.setSlotMap
        unsigned char* slotMap= (unsigned char*) malloc (head.numSlots*sizeof(unsigned char));
        recBuff.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot]=SLOT_UNOCCUPIED;
        recBuff.setSlotMap(slotMap);


        /* Decrement the numEntries in the header of the block corresponding to
           the attribute catalog entry and then set back the header
           using RecBuffer.setHeader */
        if(head.numEntries==1){
        head.numEntries--;
        recBuff.setHeader(&head);
        recBuff.releaseBlock();
        }
        else{
            head.numEntries--;
            recBuff.setHeader(&head);
        }
        

        /* If number of entries become 0, releaseBlock is called after fixing
           the linked list.
        */
        if (head.numEntries == 0) {
            /* Standard Linked List Delete for a Block
               Get the header of the left block and set it's rblock to this
               block's rblock
            */
           RecBuffer lBlock(head.lblock);
              HeadInfo head1;
                lBlock.getHeader(&head1);
                head1.rblock=head.rblock;
                lBlock.setHeader(&head1);
              



            // create a RecBuffer for lblock and call appropriate methods

            if (head.rblock!=-1) {
                /* Get the header of the right block and set it's lblock to
                   this block's lblock */
                // create a RecBuffer for rblock and call appropriate methods
                RecBuffer rBlock(head.rblock);
                HeadInfo head2;
                rBlock.getHeader(&head2);
                head2.lblock=head.lblock;
                rBlock.setHeader(&head2);

            } else {
                // (the block being released is the "Last Block" of the relation.)
                /* update the Relation Catalog entry's LastBlock field for this
                   relation with the block number of the previous block. */
                RecBuffer relCatRecBuff(RELCAT_BLOCK);
                int lastBlk = head.lblock;
                relCatRecBuff.getRecord(relCatEntryRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
                relCatEntryRecord[RELCAT_LAST_BLOCK_INDEX].nVal = lastBlk;
                relCatRecBuff.setRecord(relCatEntryRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);

                RelCatEntry setter;
                RelCacheTable::getRelCatEntry(RELCAT_RELID, &setter);
                setter.lastBlk=head.lblock;
                RelCacheTable::setRelCatEntry(RELCAT_RELID, &setter);
                
            }

            // (Since the attribute catalog will never be empty(why?), we do not
            //  need to handle the case of the linked list becoming empty - i.e
            //  every block of the attribute catalog gets released.)

            // call releaseBlock()
            recBuff.releaseBlock();
        }

        // (the following part is only relevant once indexing has been implemented)
        // if index exists for the attribute (rootBlock != -1), call bplus destroy
        if (rootBlock != -1) {
            // delete the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
            //BPlusTree::bPlusDestroy(rootBlock);
            BPlusTree::bPlusDestroy(rootBlock);
        }
    }

    /*** Delete the entry corresponding to the relation from relation catalog ***/
    // Fetch the header of Relcat block

    RecBuffer recBuff1(RELCAT_BLOCK);
    HeadInfo head;
    recBuff1.getHeader(&head);

    /* Decrement the numEntries in the header of the block corresponding to the
       relation catalog entry and set it back */
    head.numEntries--;
    recBuff1.setHeader(&head);

    /* Get the slotmap in relation catalog, update it by marking the slot as
       free(SLOT_UNOCCUPIED) and set it back. */
    unsigned char* slotMap= (unsigned char*) malloc (head.numSlots*sizeof(unsigned char));
    recBuff1.getSlotMap(slotMap);
    slotMap[relCatRecId.slot]=SLOT_UNOCCUPIED;
    recBuff1.setSlotMap(slotMap);

    /*** Updating the Relation Cache Table ***/
    /** Update relation catalog record entry (number of records in relation
        catalog is decreased by 1) **/
    // Get the entry corresponding to relation catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)
    RelCatEntry relcat;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relcat);
    relcat.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relcat);


    /** Update attribute catalog entry (number of records in attribute catalog
        is decreased by numberOfAttributesDeleted) **/
    // i.e., #Records = #Records - numberOfAttributesDeleted

    RelCatEntry attrcat;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrcat);
    attrcat.numRecs-=numberOfAttributesDeleted;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrcat);

    // Get the entry corresponding to attribute catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

    return SUCCESS;
}

int BlockAccess::project(int relId, Attribute *record) {
    // get the previous search index of the relation relId from the relation
    // cache (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    // declare block and slot which will be used to store the record id of the
    // slot we need to check.
    int block=prevRecId.block, slot=prevRecId.slot;

    /* if the current search index record is invalid(i.e. = {-1, -1})
       (this only happens when the caller reset the search index)
    */
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (new project operation. start from beginning)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);

        // block = first record block of the relation
        // slot = 0
        block = relCatEntry.firstBlk;
        slot = 0;

    }
    else
    {
        // (a project/search operation is already in progress)


        // block = previous search index's block
        // slot = previous search index's slot + 1
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }


    // The following code finds the next record of the relation
    /* Start from the record id (block, slot) and iterate over the remaining
       records of the relation */
    while (block != -1)
    {
        // create a RecBuffer object for block (using appropriate constructor!)
        RecBuffer recBuff(block);

        // get header of the block using RecBuffer::getHeader() function
        // get slot map of the block using RecBuffer::getSlotMap() function
        HeadInfo head;
        recBuff.getHeader(&head);
        unsigned char* slotMap= (unsigned char*) malloc (head.numSlots*sizeof(unsigned char));
        recBuff.getSlotMap(slotMap);

        if(slot>=head.numSlots)
        {
            // (no more slots in this block)
            // update block = right block of block
            // update slot = 0
            // (NOTE: if this is the last block, rblock would be -1. this would
            //        set block = -1 and fail the loop condition )
            block = head.rblock;
            slot = 0;
            continue;
        }
        else if (slotMap[slot]==SLOT_UNOCCUPIED)
        { // (i.e slot-th entry in slotMap contains SLOT_UNOCCUPIED)

            // increment slot
            slot++;
        }
        else {
            // (the next occupied slot / record has been found)
            break;
        }
    }

    if (block == -1){
        // (a record was not found. all records exhausted)
        return E_NOTFOUND;
    }

    // declare nextRecId to store the RecId of the record found
    RecId nextRecId;
    nextRecId.block=block;
    nextRecId.slot=slot;

    // set the search index to nextRecId using RelCacheTable::setSearchIndex
    RelCacheTable::setSearchIndex(relId, &nextRecId);

    /* Copy the record with record id (nextRecId) to the record buffer (record)
       For this Instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */
    RecBuffer recBuff(nextRecId.block);
    recBuff.getRecord(record, nextRecId.slot);


    return SUCCESS;
}
