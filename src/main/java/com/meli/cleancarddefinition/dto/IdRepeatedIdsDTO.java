package com.meli.cleancarddefinition.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class IdRepeatedIdsDTO {

    private long id;
    private List<Long> ids;

}
