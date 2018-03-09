package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.*;
import java.util.Objects;

@Entity
@Table(name = "bond_infos", schema = "", catalog = "")
@IdClass(BondInfosPK.class)
public class BondInfos {
    @Transient
    private long autoId;
    private String bondKey;
    private String listedMarket;
    private String yieldCurveCode;
    private String category;
    private String subCategory;
    private String rating;
    private String issuerCode;
    private String bondType;
    private String issuerName;
    private String shortName;
    private String bondSubType;
    private String isMunicipal;
    private String listedType;
    private String institutionSubtype;
    private String cbrcFinancingPlatform;
    private String province;

    @Transient
    @Column(name = "auto_id", nullable = false)
    public long getAutoId() {
        return autoId;
    }

    public void setAutoId(long autoId) {
        this.autoId = autoId;
    }

    @Id
    @Column(name = "bond_key", nullable = false, length = 32)
    public String getBondKey() {
        return bondKey;
    }

    public void setBondKey(String bondKey) {
        this.bondKey = bondKey;
    }

    @Id
    @Column(name = "listed_market", nullable = false, length = 4)
    public String getListedMarket() {
        return listedMarket;
    }

    public void setListedMarket(String listedMarket) {
        this.listedMarket = listedMarket;
    }

    @Basic
    @Column(name = "yield_curve_code", nullable = true, length = 32)
    public String getYieldCurveCode() {
        return yieldCurveCode;
    }

    public void setYieldCurveCode(String yieldCurveCode) {
        this.yieldCurveCode = yieldCurveCode;
    }

    @Basic
    @Column(name = "category", nullable = true, length = 15)
    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Basic
    @Column(name = "sub_category", nullable = true, length = 15)
    public String getSubCategory() {
        return subCategory;
    }

    public void setSubCategory(String subCategory) {
        this.subCategory = subCategory;
    }

    @Basic
    @Column(name = "rating", nullable = true, length = 10)
    public String getRating() {
        return rating;
    }

    public void setRating(String rating) {
        this.rating = rating;
    }

    @Basic
    @Column(name = "issuer_code", nullable = true, length = 15)
    public String getIssuerCode() {
        return issuerCode;
    }

    public void setIssuerCode(String issuerCode) {
        this.issuerCode = issuerCode;
    }

    @Basic
    @Column(name = "bond_type", nullable = true, length = 2)
    public String getBondType() {
        return bondType;
    }

    public void setBondType(String bondType) {
        this.bondType = bondType;
    }

    @Basic
    @Column(name = "issuer_name", nullable = true, length = 50)
    public String getIssuerName() {
        return issuerName;
    }

    public void setIssuerName(String issuerName) {
        this.issuerName = issuerName;
    }

    @Basic
    @Column(name = "short_name", nullable = true, length = 32)
    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    @Basic
    @Column(name = "bond_sub_type", nullable = true, length = 3)
    public String getBondSubType() {
        return bondSubType;
    }

    public void setBondSubType(String bondSubType) {
        this.bondSubType = bondSubType;
    }

    @Basic
    @Column(name = "is_municipal", nullable = true, length = 1)
    public String getIsMunicipal() {
        return isMunicipal;
    }

    public void setIsMunicipal(String isMunicipal) {
        this.isMunicipal = isMunicipal;
    }

    @Basic
    @Column(name = "listed_type", nullable = true, length = 18)
    public String getListedType() {
        return listedType;
    }

    public void setListedType(String listedType) {
        this.listedType = listedType;
    }

    @Basic
    @Column(name = "institution_subtype", nullable = true, length = 5)
    public String getInstitutionSubtype() {
        return institutionSubtype;
    }

    public void setInstitutionSubtype(String institutionSubtype) {
        this.institutionSubtype = institutionSubtype;
    }

    @Basic
    @Column(name = "cbrc_financing_platform", nullable = true, length = 5)
    public String getCbrcFinancingPlatform() {
        return cbrcFinancingPlatform;
    }

    public void setCbrcFinancingPlatform(String cbrcFinancingPlatform) {
        this.cbrcFinancingPlatform = cbrcFinancingPlatform;
    }

    @Basic
    @Column(name = "province", nullable = true, length = 5)
    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BondInfos bondInfos = (BondInfos) o;
        return autoId == bondInfos.autoId &&
                Objects.equals(bondKey, bondInfos.bondKey) &&
                Objects.equals(listedMarket, bondInfos.listedMarket) &&
                Objects.equals(yieldCurveCode, bondInfos.yieldCurveCode) &&
                Objects.equals(category, bondInfos.category) &&
                Objects.equals(subCategory, bondInfos.subCategory) &&
                Objects.equals(rating, bondInfos.rating) &&
                Objects.equals(issuerCode, bondInfos.issuerCode) &&
                Objects.equals(bondType, bondInfos.bondType) &&
                Objects.equals(issuerName, bondInfos.issuerName) &&
                Objects.equals(shortName, bondInfos.shortName) &&
                Objects.equals(bondSubType, bondInfos.bondSubType) &&
                Objects.equals(isMunicipal, bondInfos.isMunicipal) &&
                Objects.equals(listedType, bondInfos.listedType) &&
                Objects.equals(institutionSubtype, bondInfos.institutionSubtype) &&
                Objects.equals(cbrcFinancingPlatform, bondInfos.cbrcFinancingPlatform) &&
                Objects.equals(province, bondInfos.province);
    }

    @Override
    public int hashCode() {

        return Objects.hash(autoId, bondKey, listedMarket, yieldCurveCode, category, subCategory, rating, issuerCode, bondType, issuerName, shortName, bondSubType, isMunicipal, listedType, institutionSubtype, cbrcFinancingPlatform, province);
    }
}
