package com.meli.cleancarddefinition.domain;

import lombok.Data;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

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

    @ManyToMany()
    @JoinTable(name = "card_definition_payment_methods",
            joinColumns = @JoinColumn(name = "card_definition_id"),
            inverseJoinColumns = @JoinColumn(name = "payment_method_id")
    )
    private List<PaymentMethod> paymentMethods = new ArrayList<>();

}
