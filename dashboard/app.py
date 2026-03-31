import os, requests
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

API_BASE = os.environ.get("API_BASE", "http://localhost:8000")
API_USER = os.environ.get("API_USER", "admin")
API_PASS = os.environ.get("API_PASS", "secret")

st.set_page_config(page_title="Olist Data Platform", page_icon="🛒", layout="wide")

@st.cache_data(ttl=3000)
def get_token():
    r = requests.post("%s/token" % API_BASE, data={"username": API_USER, "password": API_PASS})
    r.raise_for_status()
    return r.json()["access_token"]

def api_get(endpoint):
    token = get_token()
    all_data, page = [], 1
    while True:
        r = requests.get("%s%s" % (API_BASE, endpoint),
                        headers={"Authorization": "Bearer %s" % token},
                        params={"page": page, "page_size": 100})
        r.raise_for_status()
        body = r.json()
        all_data.extend(body["data"])
        if page * body["page_size"] >= body["total"]:
            break
        page += 1
    return all_data

st.title("🛒 Olist E-Commerce – Data Platform")
tab1, tab2, tab3, tab4 = st.tabs(["📅 Revenus mensuels", "🏪 Vendeurs", "📦 Statuts", "⭐ Satisfaction"])

with tab1:
    st.subheader("Evolution du chiffre d'affaires mensuel")
    try:
        df = pd.DataFrame(api_get("/datamarts/monthly-revenue"))
        df["order_month"] = pd.to_datetime(df["order_month"])
        df = df.sort_values("order_month")
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(px.bar(df, x="order_month", y="monthly_revenue",
                title="Revenu mensuel (BRL)", color="monthly_revenue",
                color_continuous_scale="Blues",
                labels={"order_month": "Mois", "monthly_revenue": "Revenu (BRL)"}),
                use_container_width=True)
        with c2:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df["order_month"], y=df["cumulative_revenue"],
                mode="lines+markers", fill="tozeroy", name="Cumule"))
            fig.update_layout(title="Revenu cumule")
            st.plotly_chart(fig, use_container_width=True)
        k1, k2 = st.columns(2)
        k1.metric("Revenu total", "R$ {:,.0f}".format(df["monthly_revenue"].sum()))
        k2.metric("Commandes totales", "{:,.0f}".format(df["nb_orders"].sum()))
    except Exception as e:
        st.error("Erreur : %s" % e)

with tab2:
    st.subheader("Top vendeurs par revenu")
    try:
        df = pd.DataFrame(api_get("/datamarts/seller-performance")).sort_values("revenue_rank")
        n = st.slider("Top N vendeurs", 5, 50, 20)
        st.plotly_chart(px.bar(df.head(n), x="seller_id", y="total_revenue",
            color="avg_review_score", color_continuous_scale="RdYlGn", range_color=[1,5],
            title="Top %d vendeurs" % n,
            labels={"seller_id": "Vendeur", "total_revenue": "Revenu (BRL)", "avg_review_score": "Score"}),
            use_container_width=True)
        st.plotly_chart(px.scatter(df.head(200), x="total_revenue", y="avg_review_score",
            size="nb_orders", title="Revenu vs Satisfaction",
            labels={"total_revenue": "Revenu (BRL)", "avg_review_score": "Score moyen"}),
            use_container_width=True)
    except Exception as e:
        st.error("Erreur : %s" % e)

with tab3:
    st.subheader("Performance par statut de commande")
    try:
        df = pd.DataFrame(api_get("/datamarts/order-status")).sort_values("nb_orders", ascending=False)
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(px.pie(df, names="order_status", values="nb_orders",
                title="Repartition des commandes par statut"),
                use_container_width=True)
        with c2:
            st.plotly_chart(px.bar(df, x="order_status", y="total_revenue",
                title="Revenu par statut",
                labels={"order_status": "Statut", "total_revenue": "Revenu (BRL)"}),
                use_container_width=True)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error("Erreur : %s" % e)

with tab4:
    st.subheader("Distribution des avis clients")
    try:
        df = pd.DataFrame(api_get("/datamarts/review-distribution"))
        c1, c2 = st.columns(2)
        with c1:
            df_score = df.groupby("review_score")["nb_orders"].sum().reset_index()
            st.plotly_chart(px.bar(df_score, x="review_score", y="nb_orders",
                color="review_score", color_continuous_scale="RdYlGn",
                title="Distribution des scores 1 a 5",
                labels={"review_score": "Score", "nb_orders": "Nb avis"}),
                use_container_width=True)
        with c2:
            try:
                df_h = df.pivot_table(index="order_status", columns="review_score",
                    values="nb_orders", aggfunc="sum", fill_value=0)
                st.plotly_chart(px.imshow(df_h, title="Heatmap statut vs score",
                    color_continuous_scale="Blues"), use_container_width=True)
            except Exception:
                st.info("Heatmap non disponible")
    except Exception as e:
        st.error("Erreur : %s" % e)

st.caption("Olist Data Platform - M1 Data Engineering | Architecture Medallion")