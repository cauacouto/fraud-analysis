package com.example.FraudAnalysis.dto;

import com.example.FraudAnalysis.Enums.StatusMotivo;
import com.example.FraudAnalysis.Enums.StatusTransfer;

public record ResultAnalitic(StatusTransfer statusTransfer, StatusMotivo statusMotivo) {
}
