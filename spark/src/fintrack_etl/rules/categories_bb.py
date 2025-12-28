from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, List, Dict


@dataclass(frozen=True)
class CategoryRule:
    categoria: str
    subcategoria: Optional[str]
    patterns: List[str]
    force_recorrente: bool = False


RULES: List[CategoryRule] = [
    # -----------------------------
    # Educação
    # -----------------------------
    CategoryRule(
        categoria="Educação",
        subcategoria=None,
        patterns=[r"\bFIAP\b", r"\bUDEMY\b", r"\bCOURSERA\b", r"\bCURSO\b"],
        force_recorrente=False,
    ),

    # -----------------------------
    # Saúde / Bem-estar
    # -----------------------------
    CategoryRule(
        categoria="Saúde",
        subcategoria="Telemedicina/Serviços",
        patterns=[r"\bRDSAUDE\b", r"\bRDSAUDE ONLINE\b"],
        force_recorrente=True,
    ),
    CategoryRule(
        categoria="Saúde",
        subcategoria="Academia/Bem-estar",
        patterns=[r"\bWELLHUB\b", r"\bGYMPASS\b"],
        force_recorrente=True,
    ),

    # -----------------------------
    # Assinaturas / serviços digitais
    # -----------------------------
    CategoryRule(
        categoria="Assinaturas",
        subcategoria="Software/IA",
        patterns=[r"\bOPENAI\b", r"\bCHATGPT\b"],
        force_recorrente=True,
    ),
    CategoryRule(
        categoria="Assinaturas",
        subcategoria="Google/Amazon (digital)",
        patterns=[r"\bDL\*GOOGLE\b", r"\bGOOGLE\b", r"\bAMAZON\b"],
        force_recorrente=True,
    ),

    # -----------------------------
    # Compras / e-commerce / pagamentos
    # -----------------------------
    CategoryRule(
        categoria="Compras",
        subcategoria="E-commerce",
        patterns=[r"\bMERCADOLIVRE\b", r"\bMERCADO LIVRE\b"],
        force_recorrente=False,
    ),
    CategoryRule(
        categoria="Financeiro",
        subcategoria="Pagamentos/Carteira digital",
        patterns=[r"\bMERCADOPAGO\b", r"\bMERCADO PAGO\b", r"\bPAYGO\b"],
        force_recorrente=False,
    ),

    # -----------------------------
    # Mercado / alimentação em casa
    # -----------------------------
    CategoryRule(
        categoria="Mercado",
        subcategoria="Supermercado",
        patterns=[r"\bANCORA DISTRIBUIDORA\b"],
        force_recorrente=False,
    ),
    CategoryRule(
        categoria="Mercado",
        subcategoria=None,
        patterns=[
            r"\bPINHEIRO\b",
            r"\bMERCADINHO SAO LUIZ\b",
            r"\bMERCADO 901\b",
            r"\bMERCADINHO\b",
            r"\bSUPERMERCADO\b",
        ],
        force_recorrente=False,
    ),

    # -----------------------------
    # Transporte
    # -----------------------------
    CategoryRule(
        categoria="Transporte",
        subcategoria="Apps/Taxi",
        patterns=[r"\bUBER\b", r"HELP\.UBER", r"WWW\.UBER\.COM"],
        force_recorrente=False,
    ),
    CategoryRule(
        categoria="Transporte",
        subcategoria="Estacionamento",
        patterns=[
            r"\bPARKING\b",
            r"\bITC PARKING\b",
            r"\bESTACIONA\b",
            r"\bTEIXEIRA ESTACIONA\b",
            r"\bTEIXEIRA ESTACIONAME\b",
        ],
        force_recorrente=False,
    ),
    CategoryRule(
        categoria="Transporte",
        subcategoria="Combustível",
        patterns=[r"\bSOBRAL E PALACIO\b"],
        force_recorrente=False,
    ),

    # -----------------------------
    # Lazer / Esporte
    # -----------------------------
    CategoryRule(
        categoria="Lazer",
        subcategoria="Esporte / Beach Tennis",
        patterns=[r"\bHABACUC\b", r"\bHABACUCBANDEIRA\b"],
        force_recorrente=False,
    ),
    CategoryRule(
        categoria="Lazer",
        subcategoria="Shopping",
        patterns=[r"\bRIOMAR\b"],
        force_recorrente=False,
    ),
    CategoryRule(
        categoria="Lazer",
        subcategoria="Clube/Atividades",
        patterns=[r"\bSESC\b"],
        force_recorrente=False,
    ),
    CategoryRule(
        categoria="Lazer",
        subcategoria="Eventos",
        patterns=[r"\bPINK FESTAS\b", r"\bZP\*PLAY NAS FRIAS\b", r"\bPLAY NAS FRIAS\b"],
        force_recorrente=False,
    ),
    CategoryRule(
        categoria="Lazer",
        subcategoria="Esporte",
        patterns=[r"\bPODIUM BT\b", r"\bPODIUM\b"],
        force_recorrente=False,
    ),

    # -----------------------------
    # Casa / Variedades
    # -----------------------------
    CategoryRule(
        categoria="Casa",
        subcategoria="Varejo/Variedades",
        patterns=[r"\bM V VARIEDADES\b", r"\bMUNDO E CIA\b", r"\bVARIEDADES\b", r"\bMUNDO\b"],
        force_recorrente=False,
    ),

    # -----------------------------
    # Seguros
    # -----------------------------
    CategoryRule(
        categoria="Seguros",
        subcategoria="Auto",
        patterns=[r"\bBRADESCO AUT\*", r"\bTOKIO MARINE\*AUTO\b", r"\bTOKIO MARINE\b"],
        force_recorrente=True,
    ),

    # -----------------------------
    # Compras (loja física genérica)
    # -----------------------------
    CategoryRule(
        categoria="Compras",
        subcategoria="Loja física",
        patterns=[r"\bDPSSA\b", r"\bCASA BLANCA\b"],
        force_recorrente=False,
    ),

    # -----------------------------
    # Alimentação (iFood / delivery / restaurantes)
    # -----------------------------
    CategoryRule(
        categoria="Alimentação",
        subcategoria="Delivery",
        patterns=[r"\bIFD\*", r"\bIFOOD\b", r"\bRAPPI\b", r"\bUBER EATS\b"],
        force_recorrente=False,
    ),
    CategoryRule(
        categoria="Alimentação",
        subcategoria="Restaurante/Lanchonete",
        patterns=[r"\bRESTAURANT\b", r"\bGASTRONOMIA\b", r"\bDELI\b", r"\bALIMENTACAO\b"],
        force_recorrente=False,
    ),

    # -----------------------------
    # Financeiro (tarifas/juros/etc)
    # -----------------------------
    CategoryRule(
        categoria="Financeiro",
        subcategoria="Tarifas/Juros/IOF",
        patterns=[r"\bIOF\b", r"\bJUROS\b", r"\bENCARG\b", r"\bMULTA\b", r"\bTARIFA\b", r"\bANUIDADE\b"],
        force_recorrente=False,
    ),
]

RECURRENCE_HINTS = [r"\bSUBSCR\b", r"\bASSINAT\b", r"\bMENSAL\b", r"\bMONTHLY\b"]


def _norm(desc: str) -> str:
    return re.sub(r"\s+", " ", (desc or "").upper()).strip()


def categorize_bb(desc: str) -> Dict[str, object]:
    d = _norm(desc)

    parcelado = bool(re.search(r"\bPARC\s+\d{2}/\d{2}\b", d))
    recorrente = any(re.search(p, d, re.IGNORECASE) for p in RECURRENCE_HINTS)

    for rule in RULES:
        for pat in rule.patterns:
            if re.search(pat, d, re.IGNORECASE):
                if rule.force_recorrente:
                    recorrente = True
                return {
                    "categoria": rule.categoria,
                    "subcategoria": rule.subcategoria,
                    "recorrente_suspeita": recorrente,
                    "parcelado_suspeito": parcelado,
                }

    # Se é parcelado e não casou em nada, provavelmente compra parcelada em loja
    if parcelado:
        return {
            "categoria": "Compras",
            "subcategoria": "Parcelado (loja física)",
            "recorrente_suspeita": False,
            "parcelado_suspeito": True,
        }

    # Shopping / loja física genérica (quando aparece cidade no fim)
    if re.search(r"\b(FORTALEZA|SAO PAULO|EUSEBIO|SALVADOR)\b$", d, re.IGNORECASE):
        if re.search(r"\b(COMERCIO|COMÉRCIO|VAREJO|BOUTIQUE|LOJA|DISTRIBUIDORA)\b", d, re.IGNORECASE):
            return {
                "categoria": "Compras",
                "subcategoria": "Loja física",
                "recorrente_suspeita": False,
                "parcelado_suspeito": False,
            }

    # Ajustes financeiros comuns
    if re.search(r"\b(PAGAMENTO|PGTO|PAGTO|CREDITO|CR[ÉE]DITO|ESTORNO|CANCELAMENTO)\b", d, re.IGNORECASE):
        return {
            "categoria": "Financeiro",
            "subcategoria": "Pagamentos/Créditos/Estornos",
            "recorrente_suspeita": False,
            "parcelado_suspeito": parcelado,
        }

    return {
        "categoria": "Outros",
        "subcategoria": None,
        "recorrente_suspeita": recorrente,
        "parcelado_suspeito": parcelado,
    }
