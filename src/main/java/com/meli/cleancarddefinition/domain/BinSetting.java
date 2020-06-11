package com.meli.cleancarddefinition.domain;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;

@Data
@Entity(name = "bin_settings")
public class BinSetting extends BaseEntity {

    @Column
    private Long binNumber;
    @Column
    private Long cardDefinitionId;
    @Column
    private Long additionalInfoId;

}
