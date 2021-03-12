package com.meli.cleancarddefinition.domain;

import lombok.Data;

import javax.persistence.*;
import javax.smartcardio.Card;
import java.util.ArrayList;
import java.util.List;

@Data
@Entity(name = "payment_methods")
public class PaymentMethod{

    @Id
    private String id;
    @Column
    private String name;
    @Column
    private String paymentType;

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(name = "card_definition_payment_methods",
            inverseJoinColumns = @JoinColumn(name = "card_definition_id"),
            joinColumns = @JoinColumn(name = "payment_method_id")
    )
    private List<CardDefinition> cardDefinitions = new ArrayList<>();


}
