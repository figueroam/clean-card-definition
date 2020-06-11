package com.meli.cleancarddefinition.domain;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;

@Data
@Entity(name = "card_definitions")
public class CardDefinition extends BaseEntity{

    @Column
    private Long issuerId;
    @Column
    private Long cardTypeId;
    @Column
    private Long brandId;
    @Column
    private Long segmentId;

}
