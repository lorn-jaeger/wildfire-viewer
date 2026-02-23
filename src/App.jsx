import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import mapboxgl from 'mapbox-gl'
import 'mapbox-gl/dist/mapbox-gl.css'
import './App.css'

const DEFAULT_LOOKBACK_DAYS = 7
const DEFAULT_MIN_AREA_KM2 = 1
const DEFAULT_ACTIVE_LIMIT = 500
const EMPTY_GEOJSON = { type: 'FeatureCollection', features: [] }

function getTodayUtcDay() {
  return new Date().toISOString().slice(0, 10)
}

function formatUtcDateTime(value) {
  if (!value) return 'n/a'
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return 'n/a'
  return parsed.toISOString().replace('T', ' ').slice(0, 16) + ' UTC'
}

function formatNumber(value, fractionDigits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '0'
  }
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: fractionDigits,
  })
}

function parseApiError(status, payload) {
  if (payload && typeof payload.error === 'string') {
    return payload.error
  }
  return `Request failed with status ${status}`
}

function extendBoundsWithGeometry(bounds, geometry) {
  if (!geometry || !geometry.type) return

  if (geometry.type === 'Point') {
    bounds.extend(geometry.coordinates)
    return
  }

  if (geometry.type === 'LineString') {
    for (const coord of geometry.coordinates) bounds.extend(coord)
    return
  }

  if (geometry.type === 'Polygon') {
    for (const ring of geometry.coordinates) {
      for (const coord of ring) bounds.extend(coord)
    }
    return
  }

  if (geometry.type === 'MultiPoint') {
    for (const coord of geometry.coordinates) bounds.extend(coord)
    return
  }

  if (geometry.type === 'MultiLineString') {
    for (const line of geometry.coordinates) {
      for (const coord of line) bounds.extend(coord)
    }
    return
  }

  if (geometry.type === 'MultiPolygon') {
    for (const polygon of geometry.coordinates) {
      for (const ring of polygon) {
        for (const coord of ring) bounds.extend(coord)
      }
    }
  }
}

function AreaSparkline({ series }) {
  const points = useMemo(() => {
    if (!Array.isArray(series) || series.length === 0) return ''
    const width = 260
    const height = 72
    const padding = 6
    const values = series.map((item) => Number(item.areaKm2) || 0)
    const min = Math.min(...values)
    const max = Math.max(...values)
    const range = Math.max(max - min, 0.0001)

    return values
      .map((value, index) => {
        const x = padding + (index / Math.max(values.length - 1, 1)) * (width - padding * 2)
        const y = height - padding - ((value - min) / range) * (height - padding * 2)
        return `${index === 0 ? 'M' : 'L'}${x.toFixed(2)} ${y.toFixed(2)}`
      })
      .join(' ')
  }, [series])

  if (!points) {
    return <p className="muted">No perimeter timeline in this window.</p>
  }

  return (
    <svg className="sparkline" viewBox="0 0 260 72" aria-label="Fire area timeline">
      <path d={points} />
    </svg>
  )
}

function App() {
  const mapContainerRef = useRef(null)
  const mapRef = useRef(null)
  const popupRef = useRef(null)

  const [asOfDay, setAsOfDay] = useState(getTodayUtcDay())
  const [lookbackDays, setLookbackDays] = useState(DEFAULT_LOOKBACK_DAYS)
  const [minAreaKm2, setMinAreaKm2] = useState(DEFAULT_MIN_AREA_KM2)
  const [detailMode, setDetailMode] = useState('overpass')

  const [mapReady, setMapReady] = useState(false)
  const [loadingActive, setLoadingActive] = useState(false)
  const [loadingDetail, setLoadingDetail] = useState(false)
  const [activeCount, setActiveCount] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [lastActiveRequest, setLastActiveRequest] = useState('')
  const [lastDetailRequest, setLastDetailRequest] = useState('')

  const [selectedFireId, setSelectedFireId] = useState(null)
  const [selectedFireDetail, setSelectedFireDetail] = useState(null)

  const mapToken = import.meta.env.VITE_MAPBOX_TOKEN || ''
  const apiBaseUrl = (import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000').replace(/\/$/, '')
  const missingToken = !mapToken

  const clearSelectedPerimeters = useCallback(() => {
    const map = mapRef.current
    if (!map) return
    const source = map.getSource('selected-fire-perimeters')
    if (source) {
      source.setData(EMPTY_GEOJSON)
    }
  }, [])

  const loadActiveFires = useCallback(async () => {
    const map = mapRef.current
    if (!map || !asOfDay) return

    const safeLookback = Math.max(1, Math.min(Number(lookbackDays) || DEFAULT_LOOKBACK_DAYS, 30))
    const safeMinArea = Math.max(0, Number(minAreaKm2) || DEFAULT_MIN_AREA_KM2)

    const params = new URLSearchParams({
      asOf: asOfDay,
      lookbackDays: String(safeLookback),
      minAreaKm2: String(safeMinArea),
      limit: String(DEFAULT_ACTIVE_LIMIT),
      _t: String(Date.now()),
    })

    const requestUrl = `${apiBaseUrl}/active-fires?${params.toString()}`
    setLastActiveRequest(requestUrl)
    setLoadingActive(true)
    setErrorMessage('')

    try {
      const response = await fetch(requestUrl, { cache: 'no-store' })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(parseApiError(response.status, payload))
      }

      const featureCollection = {
        type: 'FeatureCollection',
        features: Array.isArray(payload.features) ? payload.features : [],
      }
      const source = map.getSource('active-fires')
      if (!source) {
        throw new Error('Map source "active-fires" is not ready yet.')
      }
      source.setData(featureCollection)
      setActiveCount(Number(payload.count ?? featureCollection.features.length))

      const visibleFireIds = new Set(
        featureCollection.features
          .map((feature) => Number(feature?.properties?.fireId))
          .filter((value) => Number.isFinite(value)),
      )
      if (selectedFireId && !visibleFireIds.has(selectedFireId)) {
        setSelectedFireId(null)
        setSelectedFireDetail(null)
        clearSelectedPerimeters()
      }

      if (featureCollection.features.length > 0) {
        const bounds = new mapboxgl.LngLatBounds()
        for (const feature of featureCollection.features) {
          if (feature.geometry?.type === 'Point') {
            bounds.extend(feature.geometry.coordinates)
          }
        }
        if (!bounds.isEmpty()) {
          map.fitBounds(bounds, { padding: 56, duration: 700, maxZoom: 8.5 })
        }
      }
    } catch (error) {
      setActiveCount(0)
      setSelectedFireId(null)
      setSelectedFireDetail(null)
      clearSelectedPerimeters()
      setErrorMessage(error instanceof Error ? error.message : 'Unable to load active fires')
    } finally {
      setLoadingActive(false)
    }
  }, [apiBaseUrl, asOfDay, clearSelectedPerimeters, lookbackDays, minAreaKm2, selectedFireId])

  const loadSelectedFireDetail = useCallback(async () => {
    const map = mapRef.current
    if (!map || !selectedFireId) {
      clearSelectedPerimeters()
      return
    }

    const safeLookback = Math.max(1, Math.min(Number(lookbackDays) || DEFAULT_LOOKBACK_DAYS, 30))
    const params = new URLSearchParams({
      asOf: asOfDay,
      lookbackDays: String(safeLookback),
      mode: detailMode,
      _t: String(Date.now()),
    })
    const requestUrl = `${apiBaseUrl}/active-fires/${selectedFireId}?${params.toString()}`
    setLastDetailRequest(requestUrl)
    setLoadingDetail(true)
    setErrorMessage('')

    try {
      const response = await fetch(requestUrl, { cache: 'no-store' })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(parseApiError(response.status, payload))
      }

      setSelectedFireDetail(payload)

      const source = map.getSource('selected-fire-perimeters')
      if (!source) {
        throw new Error('Map source "selected-fire-perimeters" is not ready yet.')
      }

      const perimeters = payload.perimeters?.features
        ? payload.perimeters
        : EMPTY_GEOJSON
      source.setData(perimeters)

      if (Array.isArray(perimeters.features) && perimeters.features.length > 0) {
        const bounds = new mapboxgl.LngLatBounds()
        for (const feature of perimeters.features) {
          extendBoundsWithGeometry(bounds, feature.geometry)
        }
        if (!bounds.isEmpty()) {
          map.fitBounds(bounds, { padding: 72, duration: 700, maxZoom: 10 })
        }
      }
    } catch (error) {
      clearSelectedPerimeters()
      setSelectedFireDetail(null)
      setErrorMessage(error instanceof Error ? error.message : 'Unable to load fire detail')
    } finally {
      setLoadingDetail(false)
    }
  }, [apiBaseUrl, asOfDay, clearSelectedPerimeters, detailMode, lookbackDays, selectedFireId])

  useEffect(() => {
    if (missingToken || !mapContainerRef.current) {
      return
    }

    mapboxgl.accessToken = mapToken
    const map = new mapboxgl.Map({
      container: mapContainerRef.current,
      style: 'mapbox://styles/mapbox/light-v11',
      center: [-98.5795, 39.8283],
      zoom: 3,
    })

    map.addControl(new mapboxgl.NavigationControl(), 'top-right')

    map.on('load', () => {
      map.addSource('active-fires', {
        type: 'geojson',
        data: EMPTY_GEOJSON,
      })
      map.addSource('selected-fire-perimeters', {
        type: 'geojson',
        data: EMPTY_GEOJSON,
      })

      map.addLayer({
        id: 'selected-fire-fill',
        type: 'fill',
        source: 'selected-fire-perimeters',
        paint: {
          'fill-color': '#f97316',
          'fill-opacity': 0.2,
        },
      })

      map.addLayer({
        id: 'selected-fire-line',
        type: 'line',
        source: 'selected-fire-perimeters',
        paint: {
          'line-color': '#c2410c',
          'line-width': 2.4,
          'line-opacity': 0.95,
        },
      })

      map.addLayer({
        id: 'active-fire-hit',
        type: 'circle',
        source: 'active-fires',
        paint: {
          'circle-radius': 16,
          'circle-opacity': 0,
        },
      })

      map.addLayer({
        id: 'active-fire-icons',
        type: 'symbol',
        source: 'active-fires',
        layout: {
          'text-field': '🔥',
          'text-size': [
            'interpolate',
            ['linear'],
            ['coalesce', ['get', 'maxAreaKm2'], 0],
            0,
            15,
            5,
            18,
            20,
            23,
            100,
            30,
          ],
          'text-allow-overlap': true,
        },
      })

      map.on('mouseenter', 'active-fire-hit', () => {
        map.getCanvas().style.cursor = 'pointer'
      })
      map.on('mouseleave', 'active-fire-hit', () => {
        map.getCanvas().style.cursor = ''
      })

      map.on('click', 'active-fire-hit', (event) => {
        const feature = event.features?.[0]
        const properties = feature?.properties ?? {}
        const fireId = Number(properties.fireId)
        if (!Number.isFinite(fireId) || !feature?.geometry?.coordinates) {
          return
        }

        setSelectedFireId(fireId)

        if (popupRef.current) {
          popupRef.current.remove()
        }

        const popupEl = document.createElement('div')
        popupEl.className = 'map-popup'
        const title = document.createElement('div')
        title.className = 'map-popup__title'
        title.textContent = `Fire #${fireId}`
        const body = document.createElement('div')
        body.className = 'map-popup__meta'
        body.innerHTML = `
          <div>Last seen: ${formatUtcDateTime(properties.lastSeen)}</div>
          <div>Max area: ${formatNumber(properties.maxAreaKm2, 2)} km²</div>
          <div>Overpasses: ${formatNumber(properties.overpassCount, 0)}</div>
        `
        popupEl.append(title, body)

        popupRef.current = new mapboxgl.Popup({ offset: 20, closeButton: true })
          .setLngLat(feature.geometry.coordinates)
          .setDOMContent(popupEl)
          .addTo(map)
      })

      setMapReady(true)
    })

    mapRef.current = map

    return () => {
      if (popupRef.current) {
        popupRef.current.remove()
        popupRef.current = null
      }
      map.remove()
      mapRef.current = null
      setMapReady(false)
    }
  }, [mapToken, missingToken])

  useEffect(() => {
    if (!mapReady) return
    void loadActiveFires()
  }, [mapReady, loadActiveFires])

  useEffect(() => {
    if (!mapReady) return
    void loadSelectedFireDetail()
  }, [mapReady, loadSelectedFireDetail])

  const onRefresh = (event) => {
    event.preventDefault()
    void loadActiveFires()
    if (selectedFireId) {
      void loadSelectedFireDetail()
    }
  }

  const latestSlices = Array.isArray(selectedFireDetail?.series)
    ? [...selectedFireDetail.series].slice(-8).reverse()
    : []

  return (
    <main className="app">
      <h1>Wildfire Viewer</h1>
      <p className="lede">
        Active fires in the selected window are shown as flame markers. Click one to inspect perimeter
        evolution and area over time.
      </p>

      <form className="controls" onSubmit={onRefresh}>
        <label>
          As of day (UTC)
          <input
            type="date"
            value={asOfDay}
            onChange={(event) => setAsOfDay(event.target.value)}
          />
        </label>
        <label>
          Lookback days
          <input
            type="number"
            min="1"
            max="30"
            value={lookbackDays}
            onChange={(event) => setLookbackDays(Number(event.target.value) || DEFAULT_LOOKBACK_DAYS)}
          />
        </label>
        <label>
          Min fire size (km²)
          <input
            type="number"
            min="0"
            step="0.1"
            value={minAreaKm2}
            onChange={(event) => setMinAreaKm2(Number(event.target.value) || 0)}
          />
        </label>
        <label>
          Perimeter mode
          <select value={detailMode} onChange={(event) => setDetailMode(event.target.value)}>
            <option value="overpass">Per overpass</option>
            <option value="daily">Daily</option>
          </select>
        </label>
        <button type="submit" disabled={missingToken || !mapReady || loadingActive}>
          {loadingActive ? 'Loading...' : 'Refresh'}
        </button>
      </form>

      <p className="status">
        Active fires loaded: <strong>{activeCount}</strong>
      </p>

      {missingToken && (
        <p className="error">
          Set <code>VITE_MAPBOX_TOKEN</code> in a root <code>.env</code> file to display the map.
        </p>
      )}

      {errorMessage && <p className="error">{errorMessage}</p>}

      <section className="layout">
        <div className="map-wrap">
          <div ref={mapContainerRef} className="map" />
          {loadingDetail && <div className="map-loading">Loading fire detail...</div>}
        </div>

        <aside className="detail-panel">
          <h2>Fire Detail</h2>
          {!selectedFireId && <p className="muted">Click a fire marker to inspect its perimeter history.</p>}

          {selectedFireId && selectedFireDetail && (
            <>
              <p className="detail-id">Fire #{selectedFireId}</p>
              <div className="metrics">
                <div>
                  <span>First seen</span>
                  <strong>{formatUtcDateTime(selectedFireDetail.summary?.firstSeen)}</strong>
                </div>
                <div>
                  <span>Last seen</span>
                  <strong>{formatUtcDateTime(selectedFireDetail.summary?.lastSeen)}</strong>
                </div>
                <div>
                  <span>Max area</span>
                  <strong>{formatNumber(selectedFireDetail.summary?.maxAreaKm2, 2)} km²</strong>
                </div>
                <div>
                  <span>Total FRP</span>
                  <strong>{formatNumber(selectedFireDetail.summary?.totalFrpSum, 1)}</strong>
                </div>
                <div>
                  <span>Overpasses</span>
                  <strong>{formatNumber(selectedFireDetail.summary?.overpassCount, 0)}</strong>
                </div>
              </div>

              <h3>Area Over Time</h3>
              <AreaSparkline series={selectedFireDetail.series || []} />

              <h3>Latest Slices</h3>
              {latestSlices.length === 0 && <p className="muted">No slices available.</p>}
              {latestSlices.length > 0 && (
                <ul className="slice-list">
                  {latestSlices.map((slice) => (
                    <li key={slice.time}>
                      <span>{formatUtcDateTime(slice.time)}</span>
                      <strong>{formatNumber(slice.areaKm2, 2)} km²</strong>
                    </li>
                  ))}
                </ul>
              )}
            </>
          )}
        </aside>
      </section>

      <p className="request-url">
        Active request: <code>{lastActiveRequest || 'None yet'}</code>
      </p>
      {selectedFireId && (
        <p className="request-url">
          Detail request: <code>{lastDetailRequest || 'None yet'}</code>
        </p>
      )}
    </main>
  )
}

export default App
